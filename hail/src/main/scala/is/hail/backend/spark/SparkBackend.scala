package is.hail.backend.spark

import is.hail.HailContext
import is.hail.backend.{Backend, BroadcastValue}
import is.hail.expr.ir._
import is.hail.io.fs.HadoopFS
import is.hail.utils._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

object SparkBackend {
  def executeJSON(ir: IR): String = HailContext.backend.executeJSON(ir)
}

class SparkBroadcastValue[T](bc: Broadcast[T]) extends BroadcastValue[T] with Serializable {
  def value: T = bc.value
}

case class SparkBackend(sc: SparkContext) extends Backend {

  var _hadoopFS: HadoopFS = null

  def broadcast[T : ClassTag](value: T): BroadcastValue[T] = new SparkBroadcastValue[T](sc.broadcast(value))

  def parallelizeAndComputeWithIndex[T : ClassTag, U : ClassTag](collection: Array[T])(f: (T, Int) => U): Array[U] = {
    val rdd = sc.parallelize[T](collection, numSlices = collection.length)
    rdd.mapPartitionsWithIndex { (i, it) =>
      val elt = it.next()
      assert(!it.hasNext)
      Iterator.single(f(elt, i))
    }.collect()
  }

  def getHadoopFS() = {
    if (_hadoopFS == null) {
      _hadoopFS = new HadoopFS(new SerializableHadoopConfiguration(sc.hadoopConfiguration))
    }
    _hadoopFS
  }

  override def asSpark(): SparkBackend = this
}
