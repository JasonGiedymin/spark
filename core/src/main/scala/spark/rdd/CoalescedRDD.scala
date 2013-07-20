package spark.rdd

import java.io.{ObjectOutputStream, IOException}

import spark.{Dependency, NarrowDependency, RDD, Partition, TaskContext, Utils, Locality}
import scala.Array


private[spark] case class CoalescedRDDPartition(
  index: Int,
  @transient rdd: RDD[_],
  parentsIndices: Array[Int]
) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }
}

trait LocallyBasedRDD extends Locality {
  override protected def getPreferredLocations(split: Partition): Seq[String] = Seq[String](
    Utils.localIpAddressHostname
  )
}

trait Coalescence {
  type A
  @transient var prev: RDD[A]
  var maxPartitions: Int

  def getPartitions: Array[Partition] = {
    val prevSplits = prev.partitions
    if (prevSplits.length < maxPartitions) {
      // The number of previous partitions is smaller than maxPartitions. In this case, each
      // coalesced partition contains only one partition from the parent.
      prevSplits.map { partition =>
        CoalescedRDDPartition(partition.index, prev, Array(partition.index))
      }
    } else {
      (0 until maxPartitions).map { i =>
        val rangeStart = ((i.toLong * prevSplits.length) / maxPartitions).toInt
        val rangeEnd = (((i.toLong + 1) * prevSplits.length) / maxPartitions).toInt
        new CoalescedRDDPartition(i, prev, (rangeStart until rangeEnd).toArray)
      }.toArray
      //coalescePartitionsWithLocality(prev, maxPartitions)
    }
  }

  /**
   * Gets preferred locations, using byPlacement as the default curry.
   */
  def getPreferredLocations(rdd: RDD[_], partition: Int, byChoice:DepenencyPredicate=nonEmpty): Seq[String] = {

    // Lazy projection of preferred locations
    def dependencyLocations: Seq[Seq[String]] = for {
      dep <- rdd.dependencies if (dep.isInstanceOf[NarrowDependency[_]])
      parents <- dep.asInstanceOf[NarrowDependency[_]].getParents(partition)
    } yield getPreferredLocations(dep.rdd, parents, byChoice)

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dep
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.preferredLocations(rdd.partitions(partition)) match {
      case Nil => findFirst(dependencyLocations, byChoice).getOrElse{ Nil }

      // If the RDD has some placement preferences (as is the case for input RDDs), get those
      case locations => locations
    }
  }

  def coalescePartitionsWithLocality(rdd: RDD[_], maxPartitions: Int): Array[Partition] = {

    // Group all partitions by their first preferred location.
    val partsByLocations: Map[String, Seq[Int]] = Range(0, rdd.partitions.length).groupBy { part =>
      getPreferredLocations(rdd, part) match {
        case firstLoc :: otherLocs => firstLoc
        case Nil => ""
      }
    }

    def coalesce(iter: Iterator[(String, Seq[Int])]):Array[Partition] = {
      val parentIndices = new Array[Array[Int]](maxPartitions)
      val numPerPartition = math.ceil(rdd.partitions.length.toDouble / maxPartitions).toInt
      var currentLocation = iter.next()
      var currentPartitions = currentLocation._2.iterator

      var i = 0
      while (i < maxPartitions) {
        val numPartsInBucket =
          if (i == maxPartitions - 1) {
            rdd.partitions.length - numPerPartition * (maxPartitions - 1)
          } else {
            numPerPartition
          }
        parentIndices(i) = new Array[Int](numPartsInBucket)

        var j = 0
        while (j < numPartsInBucket && (currentPartitions.hasNext || iter.hasNext)) {
          if (!currentPartitions.hasNext) {
            currentLocation = iter.next()
            currentPartitions = currentLocation._2.iterator
          }
          parentIndices(i)(j) = currentPartitions.next()
          j += 1
        }

        i += 1
      }

      Array.tabulate(maxPartitions) {
        i => new CoalescedRDDPartition(i, rdd, parentIndices(i))
      }
    }

    val iter: Iterator[(String, Seq[Int])] = partsByLocations.iterator
    if (iter.isEmpty) Array.empty
    else coalesce(iter)
  }

  private type DepenencyPredicate = (Seq[String]) => Boolean
  def nonEmpty(x:Seq[String]):Boolean = x.nonEmpty
  def findFirst(projection:Seq[Seq[String]], byChoice:DepenencyPredicate=nonEmpty):Option[Seq[String]] =
    projection.find(byChoice)
  //def byTransferSize(x:Seq[String]) = false
  //def bySubnet(localIp:String) /* local rack - check if is part of subnet */

}

/**
 * Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
 * this RDD computes one or more of the parent ones. Will produce exactly `maxPartitions` if the
 * parent had more than this many partitions, or fewer if the parent had fewer.
 *
 * This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
 * or to avoid having a large number of small tasks when processing a directory with many files.
 */
class CoalescedRDD[T: ClassManifest](
  @transient var prev: RDD[T],
  var maxPartitions: Int
) extends RDD[T](prev.context, Nil) with Coalescence {
  // Nil since we implement getDependencies

  override type A = T

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap{firstParent[T].iterator(_, context)}
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

case object CoalescedRDD {
  def apply(rdd:RDD[_]): CoalescedRDD[_] = rdd.asInstanceOf[CoalescedRDD[_]]
}

