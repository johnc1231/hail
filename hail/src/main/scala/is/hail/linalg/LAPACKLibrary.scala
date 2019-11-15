package is.hail.linalg

import java.lang.reflect.Method

import com.sun.jna.{FunctionMapper, Library, Native, NativeLibrary, Pointer}
import com.sun.jna.ptr.{DoubleByReference, IntByReference}

class UnderscoreFunctionMapper extends FunctionMapper {
  override def getFunctionName(library: NativeLibrary, method: Method): String = {
    method.getName() + "_"
  }
}

object LAPACKLibrary {
  lazy val instance = {
    val jmap = new java.util.HashMap[String, FunctionMapper]()
    jmap.put(Library.OPTION_FUNCTION_MAPPER, new UnderscoreFunctionMapper)
    val foo = Native.loadLibrary("lapack", classOf[LAPACKLibrary], jmap)
    //Native.loadLibrary("lapack", classOf[LAPACKLibrary]).asInstanceOf[LAPACKLibrary]
    foo.asInstanceOf[LAPACKLibrary]
  }
  def getInstance() = instance
}

trait LAPACKLibrary extends Library {
  def dsyevd(JOBZ: Char, UPLO: Char, N: Int, A: Pointer, LDA: Int, W: Pointer, WORK: Pointer, LWORK: Int, IWORK: Pointer, LIWORK: Int, INFO: Int)
  def dlapy2(X: DoubleByReference, Y: DoubleByReference): Double
  def dgeqrf(M: Long, N: Long, A: Long, LDA: Long, TAU: Long, WORK: Long, LWORK: Long, INFO: Long)
  def ilaver(MAJOR: IntByReference, MINOR: IntByReference, PATCH: IntByReference)
}
//
//trait LAPACKLibraryUnderscore extends Library {
//  def dgeqrf_(M: Long, N: Long, A: Long, LDA: Long, TAU: Long, WORK: Long, LWORK: Long, INFO: Long)
//  def ilaver_(MAJOR: IntByReference, MINOR: IntByReference, PATCH: IntByReference)
//}