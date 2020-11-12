package is.hail.backend

import is.hail.annotations.RegionPool

object HailTaskContext {
  def get(): HailTaskContext = taskContext.get

  private[this] val taskContext: ThreadLocal[HailTaskContext] = new ThreadLocal[HailTaskContext]
  def setTaskContext(tc: HailTaskContext): Unit = taskContext.set(tc)
  def unset(): Unit = {
    taskContext.get().getRegionPool().close()
    taskContext.remove()
  }
}

abstract class HailTaskContext {
  type BackendType
  def stageId(): Int
  def partitionId(): Int
  def attemptNumber(): Int

  private lazy val thePool = RegionPool()

  def getRegionPool(): RegionPool = thePool

  def partSuffix(): String = {
    val rng = new java.security.SecureRandom()
    val fileUUID = new java.util.UUID(rng.nextLong(), rng.nextLong())
    s"${ stageId() }-${ partitionId() }-${ attemptNumber() }-$fileUUID"
  }
}