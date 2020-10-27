package is.hail.types.physical
import is.hail.annotations.{CodeOrdering, Region, StagedRegionValueBuilder, UnsafeOrdering}
import is.hail.asm4s.{Code, Value}
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder}
import is.hail.utils._
import is.hail.asm4s.{Code, _}
import is.hail.types.virtual.Type

class PReferenceCountedNDArray(val elementType: PType, val nDims: Int, val required: Boolean = false) extends PNDArray {
  @transient lazy val shape = new StaticallyKnownField(
    PCanonicalTuple(true, Array.tabulate(nDims)(_ => PInt64Required):_*): PTuple,
    off => 0L
  )

  @transient lazy val strides = new StaticallyKnownField(
    PCanonicalTuple(true, Array.tabulate(nDims)(_ => PInt64Required):_*): PTuple,
    (off) => 0L
  )

  override val data: StaticallyKnownField[PArray, Long] = ???
  override val representation: PStruct = ???

  override def numElements(shape: IndexedSeq[Value[Long]], mb: EmitMethodBuilder[_]): Code[Long] = ???

  override def makeShapeBuilder(shapeArray: IndexedSeq[Value[Long]]): StagedRegionValueBuilder => Code[Unit] = ???

  override def setElement(indices: IndexedSeq[Value[Long]], ndAddress: Value[Long], newElement: Code[_], mb: EmitMethodBuilder[_]): Code[Unit] = ???

  override def loadElementToIRIntermediate(indices: IndexedSeq[Value[Long]], ndAddress: Value[Long], mb: EmitMethodBuilder[_]): Code[_] = ???

  override def linearizeIndicesRowMajor(indices: IndexedSeq[Code[Long]], shapeArray: IndexedSeq[Value[Long]], mb: EmitMethodBuilder[_]): Code[Long] = ???

  override def unlinearizeIndexRowMajor(index: Code[Long], shapeArray: IndexedSeq[Value[Long]], mb: EmitMethodBuilder[_]): (Code[Unit], IndexedSeq[Value[Long]]) = ???

  override def construct(shapeBuilder: StagedRegionValueBuilder => Code[Unit], stridesBuilder: StagedRegionValueBuilder => Code[Unit], data: Code[Long], mb: EmitMethodBuilder[_], region: Value[Region]): Code[Long] = {
//    val sizeInBytes = mb.genFieldThisRef[Long]("ndarray_construct_size_in_bytes")
//    val dataCode = PCode(this.data.pType, data)

//    EmitCodeBuilder.scopedCode[Long](mb){cb =>
//      val dataValue = dataCode.memoize(cb, "ndarray_construct_data")
//      val dataPt = this.data.pType.asInstanceOf[PArray]
//      cb.assign(sizeInBytes,this.shape.pType.byteSize + this.strides.pType.byteSize + dataPt.byteSize + dataPt.contentsByteSize(dataValue.asIndexable.loadLength()))
//      val addr =  region.allocateNDArray(sizeInBytes)
//      dataPt.constructAtAddress()
//    }
    ???
  }

  def allocateAndInitialize(cb: EmitCodeBuilder, region: Value[Region], shapeValue: PBaseStructValue, stridesValue: PBaseStructValue): Value[Long] = {
    val sizeInBytes = cb.newField[Long]("ndarray_initialize_size_in_bytes")
    val numElements = (0 until nDims).map(i => shapeValue.loadField(cb, i).get(cb).tcode[Long]).foldLeft(const(1L).get)((a, b) => a * b)

    cb.assign(sizeInBytes, numElements * elementType.byteSize + shape.pType.byteSize + strides.pType.byteSize)
    val ndAddr = cb.newField[Long]("ndarray_addr_alloc_and_init")
    cb.assign(ndAddr, region.allocateNDArray(sizeInBytes))

    (0 until nDims).map(i => {
      cb.append(Region.storeLong(ndAddr + (8 * i), shapeValue.loadField(cb, i).get(cb).tcode[Long]))
      cb.append(Region.storeLong(ndAddr + shape.pType.byteSize + (8 * i), shapeValue.loadField(cb, i).get(cb).tcode[Long]))
    })

    ndAddr
  }

  def elementsAddress(ndAddr: Code[Long]): Code[Long] = {
    ndAddr + shape.pType.byteSize + strides.pType.byteSize
  }

  override def unsafeOrdering(): UnsafeOrdering = ???

  override def _asIdent: String = ???

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = ???

  override def encodableType: PType = ???

  override def setRequired(required: Boolean): PType = ???

  override def containsPointers: Boolean = ???

  override def copyFromType(mb: EmitMethodBuilder[_], region: Value[Region], srcPType: PType, srcAddress: Code[Long], deepCopy: Boolean): Code[Long] = ???

  override def copyFromTypeAndStackValue(mb: EmitMethodBuilder[_], region: Value[Region], srcPType: PType, stackValue: Code[_], deepCopy: Boolean): Code[_] = ???

  override protected def _copyFromAddress(region: Region, srcPType: PType, srcAddress: Long, deepCopy: Boolean): Long = ???

  override def constructAtAddress(mb: EmitMethodBuilder[_], addr: Code[Long], region: Value[Region], srcPType: PType, srcAddress: Code[Long], deepCopy: Boolean): Code[Unit] = ???

  override def constructAtAddress(addr: Long, region: Region, srcPType: PType, srcAddress: Long, deepCopy: Boolean): Unit = ???

  override def codeOrdering(mb: EmitMethodBuilder[_], other: PType): CodeOrdering = ???

  override def makeRowMajorStridesBuilder(sourceShapeArray: IndexedSeq[Value[Long]], mb: EmitMethodBuilder[_]): StagedRegionValueBuilder => Code[Unit] = ???

  override def makeColumnMajorStridesBuilder(sourceShapeArray: IndexedSeq[Value[Long]], mb: EmitMethodBuilder[_]): StagedRegionValueBuilder => Code[Unit] = ???
}
