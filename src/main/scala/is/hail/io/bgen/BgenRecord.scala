package is.hail.io.bgen

import breeze.linalg.DenseVector
import is.hail.annotations._
import is.hail.io.{ByteArrayReader, KeySerializedValueRecord}
import is.hail.utils._
import is.hail.variant.{Genotype, Variant}

abstract class BgenRecord extends KeySerializedValueRecord[Variant, Iterable[Annotation]] {
  var ann: Annotation = _

  def setAnnotation(ann: Annotation) {
    this.ann = ann
  }

  def getAnnotation: Annotation = ann

  override def getValue: Iterable[Annotation]
}

class BgenRecordV11(compressed: Boolean,
  nSamples: Int,
  tolerance: Double) extends BgenRecord {

  override def getValue: Iterable[Annotation] = {
    require(input != null, "called getValue before serialized value was set")

    val byteReader = new ByteArrayReader(if (compressed) decompress(input, nSamples * 6) else input)
    assert(byteReader.length == nSamples * 6)

    val lowerTol = (32768 * (1.0 - tolerance) + 0.5).toInt
    val upperTol = (32768 * (1.0 + tolerance) + 0.5).toInt
    assert(lowerTol > 0)

    val t = new Array[Int](3)

    new Iterable[Annotation] {
      def iterator = new Iterator[Annotation] {
        var i = 0
        byteReader.seek(0)

        def hasNext: Boolean = i < byteReader.length

        def next(): Annotation = {
          val d0 = byteReader.readShort()
          val d1 = byteReader.readShort()
          val d2 = byteReader.readShort()

          i += 6

          val dsum = d0 + d1 + d2
          if (dsum >= lowerTol
            && dsum <= upperTol) {
            t(0) = d0
            t(1) = d1
            t(2) = d2
            val gt = Genotype.unboxedGTFromLinear(t)
            val gp: IndexedSeq[Double] = Array(d0.toDouble / dsum, d1.toDouble / dsum, d2.toDouble / dsum)
            Annotation(gt, gp)
          } else
            null
        }
      }
    }
  }
}

class BGen12ProbabilityArray(a: Array[Byte], nSamples: Int, nGenotypes: Int, nBitsPerProb: Int) {

  def apply(s: Int, gi: Int): UInt = {
    assert(s >= 0 && s < nSamples)
    assert(gi >= 0 && gi < nGenotypes - 1)

    val firstBit = (s * (nGenotypes - 1) + gi) * nBitsPerProb

    var byteIndex = nSamples + 10 + (firstBit >> 3)
    var r = (a(byteIndex) & 0xff) >>> (firstBit & 7)
    var rBits = 8 - (firstBit & 7)
    byteIndex += 1

    while (rBits < nBitsPerProb) {
      r |= ((a(byteIndex) & 0xff) << rBits)
      byteIndex += 1
      rBits += 8
    }

    // clear upper bits that might have garbage from last byte or
    UInt.uintFromRep(r & ((1L << nBitsPerProb) - 1).toInt)
  }
}

final class Bgen12GenotypeIterator(a: Array[Byte],
  val nAlleles: Int,
  val nBitsPerProb: Int,
  nSamples: Int) extends Iterable[Annotation] {
  private val nGenotypes = triangle(nAlleles)

  private val totalProb = ((1L << nBitsPerProb) - 1).toUInt

  private val sampleProbs = ArrayUInt(nGenotypes)

  private val pa = new BGen12ProbabilityArray(a, nSamples, nGenotypes, nBitsPerProb)

  def isSampleMissing(s: Int): Boolean = (a(8 + s) & 0x80) != 0

  def iterator: Iterator[Annotation] = new Iterator[Annotation] {
    var sampleIndex = 0

    def hasNext: Boolean = sampleIndex < nSamples

    def next(): Annotation = {
      val g = if (isSampleMissing(sampleIndex))
        null
      else {
        var i = 0
        var lastProb = totalProb
        while (i < nGenotypes - 1) {
          val p = pa(sampleIndex, i)
          sampleProbs(i) = p
          i += 1
          lastProb -= p
        }
        sampleProbs(i) = lastProb

        val gt = Genotype.unboxedGTFromUIntLinear(sampleProbs)

        val gp: IndexedSeq[Double] = Array.tabulate(nGenotypes)(i => sampleProbs(i).toDouble / totalProb.toDouble)
        Annotation(gt, gp)
      }

      sampleIndex += 1

      g
    }
  }
}

class BgenRecordV12(compressed: Boolean, nSamples: Int, tolerance: Double) extends BgenRecord {
  var expectedDataSize: Int = _
  var expectedNumAlleles: Int = _

  def setExpectedDataSize(size: Int) {
    this.expectedDataSize = size
  }

  def setExpectedNumAlleles(n: Int) {
    this.expectedNumAlleles = n
  }

  override def getValue: Iterable[Annotation] = {
    require(input != null, "called getValue before serialized value was set")

    val a = if (compressed) decompress(input, expectedDataSize) else input
    val reader = new ByteArrayReader(a)

    val nRow = reader.readInt()
    assert(nRow == nSamples, "row nSamples is not equal to header nSamples")

    val nAlleles = reader.readShort()
    assert(nAlleles == expectedNumAlleles, s"Value for `nAlleles' in genotype probability data storage is not equal to value in variant identifying data. Expected $expectedNumAlleles but found $nAlleles.")

    val minPloidy = reader.read()
    val maxPloidy = reader.read()

    if (minPloidy != 2 || maxPloidy != 2)
      fatal(s"Hail only supports diploid genotypes. Found min ploidy equals `$minPloidy' and max ploidy equals `$maxPloidy'.")

    var i = 0
    while (i < nSamples) {
      val ploidy = reader.read()
      assert((ploidy & 0x3f) == 2, s"Ploidy value must equal to 2. Found $ploidy.")
      i += 1
    }
    assert(i == nSamples, s"Number of ploidy values `$i' does not equal the number of samples `$nSamples'.")

    val phase = reader.read()
    assert(phase == 0 || phase == 1, s"Value for phase must be 0 or 1. Found $phase.")
    val isPhased = phase == 1

    if (isPhased)
      fatal("Hail does not support phased genotypes.")

    val nBitsPerProb = reader.read()
    assert(nBitsPerProb >= 1 && nBitsPerProb <= 32, s"Value for nBits must be between 1 and 32 inclusive. Found $nBitsPerProb.")

    val nGenotypes = triangle(nAlleles)
    val nExpectedBytesProbs = (nSamples * (nGenotypes - 1) * nBitsPerProb + 7) / 8
    assert(reader.length == nExpectedBytesProbs + nSamples + 10, s"Number of uncompressed bytes `${ reader.length }' does not match the expected size `$nExpectedBytesProbs'.")

    new Bgen12GenotypeIterator(a, nAlleles, nBitsPerProb, nSamples)
  }
}

