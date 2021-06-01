package is.hail.types.physical.stypes.interfaces

import is.hail.asm4s.Code
import is.hail.expr.ir.{EmitCodeBuilder, IEmitCode}
import is.hail.types.physical.stypes.concrete.{SSubsetStruct, SSubsetStructCode}
import is.hail.types.physical.stypes._
import is.hail.types.virtual.TBaseStruct

trait SBaseStruct extends SType {
  def virtualType: TBaseStruct

  override def fromCodes(codes: IndexedSeq[Code[_]]): SBaseStructCode

  def size: Int

  val fieldTypes: IndexedSeq[SType]
  val fieldEmitTypes: IndexedSeq[EmitType]

  def fieldIdx(fieldName: String): Int
}

trait SStructSettable extends SBaseStructValue with SSettable

trait SBaseStructValue extends SValue {
  def st: SBaseStruct

  def isFieldMissing(fieldIdx: Int): Code[Boolean]

  def isFieldMissing(fieldName: String): Code[Boolean] = isFieldMissing(st.fieldIdx(fieldName))

  def loadField(cb: EmitCodeBuilder, fieldIdx: Int): IEmitCode

  def loadField(cb: EmitCodeBuilder, fieldName: String): IEmitCode = loadField(cb, st.fieldIdx(fieldName))
}

trait SBaseStructCode extends SCode { self =>
  def st: SBaseStruct

  def memoize(cb: EmitCodeBuilder, name: String): SBaseStructValue

  def memoizeField(cb: EmitCodeBuilder, name: String): SBaseStructValue

  final def loadSingleField(cb: EmitCodeBuilder, fieldName: String): IEmitCode = loadSingleField(cb, st.fieldIdx(fieldName))

  def loadSingleField(cb: EmitCodeBuilder, fieldIdx: Int): IEmitCode = {
    memoize(cb, "structcode_loadsinglefield")
      .loadField(cb, fieldIdx)
  }

  def subset(fieldNames: String*): SBaseStructCode = {
    val st = SSubsetStruct(self.st, fieldNames.toIndexedSeq)
    new SSubsetStructCode(st, self)
  }
}
