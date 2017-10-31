package is.hail.variant

import is.hail.annotations.{Annotation, Querier, RegionValue, UnsafeRow}
import is.hail.expr.{TString, TStruct}
import is.hail.io.vcf.ExportVCF
import is.hail.methods.VEP
import is.hail.utils._

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag

class VariantKeyDatasetFunctions[T >: Null](private val vsm: VariantSampleMatrix[Locus, Variant, T]) {
  implicit val tct: ClassTag[T] = vsm.tct

  /**
    *
    * @param path     output path
    * @param append   append file to header
    * @param parallel export VCF in parallel using the path argument as a directory
    */
  def exportVCF(path: String, append: Option[String] = None, parallel: Boolean = false) {
    ExportVCF(vsm, path, append, parallel)
  }

  def minRep(maxShift: Int = 100): VariantSampleMatrix[Locus, Variant, T] = {
    require(maxShift > 0, s"invalid value for maxShift: $maxShift. Parameter must be a positive integer.")
    val minrepped = vsm.rdd.map { case (v, (va, gs)) =>
      (v.minRep, (va, gs))
    }
    vsm.copy(rdd = minrepped.smartShuffleAndSort(vsm.rdd.orderedPartitioner, maxShift))
  }

  /**
    *
    * @param config    VEP configuration file
    * @param root      Variant annotation path to store VEP output
    * @param csq       Annotates with the VCF CSQ field as a string, rather than the full nested struct schema
    * @param blockSize Variants per VEP invocation
    */
  def vep(config: String, root: String = "va.vep", csq: Boolean = false,
    blockSize: Int = 1000): VariantSampleMatrix[Locus, Variant, T] = {
    VEP.annotate(vsm, config, root, csq, blockSize)
  }

  def filterIntervals(intervals: java.util.ArrayList[Interval[Locus]], keep: Boolean): VariantSampleMatrix[Locus, Variant, T] = {
    val iList = IntervalTree[Locus](intervals.asScala.toArray)
    filterIntervals(iList, keep)
  }

  def filterIntervals(iList: IntervalTree[Locus, _], keep: Boolean): VariantSampleMatrix[Locus, Variant, T] = {
    if (keep)
      vsm.copy(rdd = vsm.rdd.filterIntervals(iList))
    else {
      val iListBc = vsm.sparkContext.broadcast(iList)
      vsm.filterVariants { (v, va, gs) => !iListBc.value.contains(v.locus) }
    }
  }

  /**
    * Remove multiallelic variants from this dataset.
    *
    * Useful for running methods that require biallelic variants without calling the more expensive split_multi step.
    */
  def filterMulti(): VariantSampleMatrix[Locus, Variant, T] = {
    if (vsm.wasSplit) {
      warn("called redundant `filter_multi' on an already split or multiallelic-filtered VDS")
      vsm
    } else {
      vsm.filterVariants {
        case (v, va, gs) => v.isBiallelic
      }.copy2(wasSplit = true)
    }
  }

  def verifyBiallelic(): VariantSampleMatrix[Locus, Variant, T] =
    verifyBiallelic("verifyBiallelic")

  def verifyBiallelic(method: String): VariantSampleMatrix[Locus, Variant, T] = {
    if (vsm.wasSplit) {
      warn("called redundant `$method' on biallelic VDS")
      vsm
    } else {
      val localRowType = vsm.rowType
      vsm.copy2(
        rdd2 = vsm.rdd2.mapPreservesPartitioning { rv =>
          val ur = new UnsafeRow(localRowType, rv.region, rv.offset)
          val v = ur.getAs[Variant](1)
          if (!v.isBiallelic)
            fatal("in $method: found non-biallelic variant: $v")
          rv
        },
        wasSplit = true)
    }
  }

  def exportGen(path: String, precision: Int = 4) {
    require(vsm.wasSplit)

    def writeSampleFile() {
      // FIXME: should output all relevant sample annotations such as phenotype, gender, ...
      vsm.hc.hadoopConf.writeTable(path + ".sample",
        "ID_1 ID_2 missing" :: "0 0 0" :: vsm.sampleIds.map(s => s"$s $s 0").toList)
    }

    def writeGenFile() {
      val varidSignature = vsm.vaSignature.getOption("varid")
      val varidQuery: Querier = varidSignature match {
        case Some(_) =>
          val (t, q) = vsm.queryVA("va.varid")
          t match {
            case TString => q
            case _ => a => null
          }
        case None => a => null
      }

      val rsidSignature = vsm.vaSignature.getOption("rsid")
      val rsidQuery: Querier = rsidSignature match {
        case Some(_) =>
          val (t, q) = vsm.queryVA("va.rsid")
          t match {
            case TString => q
            case _ => a => null
          }
        case None => a => null
      }

      val localNSamples = vsm.nSamples
      val localRowType = vsm.rowType
      vsm.rdd2.mapPartitions { it =>
        val sb = new StringBuilder
        val view = new ArrayGenotypeView(localRowType)
        it.map { rv =>
          view.setRegion(rv)
          val ur = new UnsafeRow(localRowType, rv)

          val v = ur.getAs[Variant](1)
          val va = ur.get(2)

          sb.clear()
          sb.append(v.contig)
          sb += ' '
          sb.append(Option(varidQuery(va)).getOrElse(v.toString))
          sb += ' '
          sb.append(Option(rsidQuery(va)).getOrElse("."))
          sb += ' '
          sb.append(v.start)
          sb += ' '
          sb.append(v.ref)
          sb += ' '
          sb.append(v.alt)

          var i = 0
          while (i < localNSamples) {
            view.setGenotype(i)
            if (view.hasGP) {
              sb += ' '
              sb.append(formatDouble(view.getGP(0), precision))
              sb += ' '
              sb.append(formatDouble(view.getGP(1), precision))
              sb += ' '
              sb.append(formatDouble(view.getGP(2), precision))
            } else
              sb.append(" 0 0 0")
            i += 1
          }
          sb.result()
        }
      }.writeTable(path + ".gen", vsm.hc.tmpDir, None)
    }

    writeSampleFile()
    writeGenFile()
  }
}
