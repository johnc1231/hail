package is.hail.variant

import is.hail.annotations.{Annotation, _}
import is.hail.expr.{EvalContext, Parser, TAggregable, TString, TStruct, Type, _}
import is.hail.io.plink.ExportBedBimFam
import is.hail.keytable.KeyTable
import is.hail.methods._
import is.hail.stats.ComputeRRM
import is.hail.utils._
import is.hail.variant.Variant.orderedKey
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions

object VariantDataset {

  def fromKeyTable(kt: KeyTable): VariantDataset = {
    kt.keyFields.map(_.typ) match {
      case Array(TVariant(_)) =>
      case arr => fatal("Require one key column of type Variant to produce a variant dataset, " +
        s"but found [ ${ arr.mkString(", ") } ]")
    }

    val rdd = kt.keyedRDD()
      .map { case (k, v) => (k.asInstanceOf[Row].getAs[Variant](0), v) }
      .filter(_._1 != null)
      .mapValues(a => (a: Annotation, Iterable.empty[Genotype]))
      .toOrderedRDD

    val metadata = VSMMetadata(
      saSignature = TStruct.empty,
      vaSignature = kt.valueSignature,
      globalSignature = TStruct.empty)

    new VariantSampleMatrix[Locus, Variant, Genotype](kt.hc, metadata,
      VSMLocalValue(Annotation.empty, Array.empty[Annotation], Array.empty[Annotation]), rdd)
  }
}

class VariantDatasetFunctions(private val vds: VariantDataset) extends AnyVal {

  def annotateAllelesExpr(expr: String, propagateGQ: Boolean = false): VariantDataset = {
    val (vas2, insertIndex) = vds.vaSignature.insert(TInt32, "aIndex")
    val (vas3, insertSplit) = vas2.insert(TBoolean, "wasSplit")
    val localGlobalAnnotation = vds.globalAnnotation

    val aggregationST = Map(
      "global" -> (0, vds.globalSignature),
      "v" -> (1, TVariant(GenomeReference.GRCh37)),
      "va" -> (2, vas3),
      "g" -> (3, TGenotype),
      "s" -> (4, TString),
      "sa" -> (5, vds.saSignature))
    val ec = EvalContext(Map(
      "global" -> (0, vds.globalSignature),
      "v" -> (1, TVariant(GenomeReference.GRCh37)),
      "va" -> (2, vas3),
      "gs" -> (3, TAggregable(TGenotype, aggregationST))))

    val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, Some(Annotation.VARIANT_HEAD))

    val inserterBuilder = new ArrayBuilder[Inserter]()
    val newType = (paths, types).zipped.foldLeft(vds.vaSignature) { case (vas, (ids, signature)) =>
      val (s, i) = vas.insert(TArray(signature), ids)
      inserterBuilder += i
      s
    }
    val finalType = if (newType.isInstanceOf[TStruct])
      paths.foldLeft(newType.asInstanceOf[TStruct]) {
        case (res, path) => res.setFieldAttributes(path, Map("Number" -> "A"))
      }
    else newType

    val inserters = inserterBuilder.result()

    val aggregateOption = Aggregators.buildVariantAggregations(vds, ec)

    vds.mapAnnotations(finalType, { case (v, va, gs) =>

      val annotations = SplitMulti.split(v, va, gs,
        propagateGQ = propagateGQ,
        keepStar = true,
        insertSplitAnnots = { (va, index, wasSplit) =>
          insertSplit(insertIndex(va, index), wasSplit)
        },
        f = _ => true)
        .map({
          case (v, (va, gs)) =>
            ec.setAll(localGlobalAnnotation, v, va)
            aggregateOption.foreach(f => f(v, va, gs))
            f()
        }).toArray

      inserters.zipWithIndex.foldLeft(va) {
        case (va, (inserter, i)) =>
          inserter(va, annotations.map(_ (i)).toArray[Any]: IndexedSeq[Any])
      }

    })
  }

  def concordance(other: VariantDataset): (IndexedSeq[IndexedSeq[Long]], KeyTable, KeyTable) = {
    require(vds.wasSplit)
    require(other.wasSplit)

    CalculateConcordance(vds, other)
  }

  def summarize(): SummaryResult = {
    vds.rdd
      .aggregate(new SummaryCombiner[Genotype](_.hardCallIterator.countNonNegative()))(_.merge(_), _.merge(_))
      .result(vds.nSamples)
  }

  def exportGen(path: String, precision: Int = 4) {
    require(vds.wasSplit)

    def writeSampleFile() {
      //FIXME: should output all relevant sample annotations such as phenotype, gender, ...
      vds.hc.hadoopConf.writeTable(path + ".sample",
        "ID_1 ID_2 missing" :: "0 0 0" :: vds.sampleIds.map(s => s"$s $s 0").toList)
    }


    def formatGP(d: Double): String = d.formatted(s"%.${ precision }f")

    val emptyGP = Array(0d, 0d, 0d)

    def appendRow(sb: StringBuilder, v: Variant, va: Annotation, gs: Iterable[Genotype], rsidQuery: Querier, varidQuery: Querier) {
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

      for (g <- gs) {
        val gp = Genotype.gp(g).getOrElse(emptyGP)
        sb += ' '
        sb.append(formatGP(gp(0)))
        sb += ' '
        sb.append(formatGP(gp(1)))
        sb += ' '
        sb.append(formatGP(gp(2)))
      }
    }

    def writeGenFile() {
      val varidSignature = vds.vaSignature.getOption("varid")
      val varidQuery: Querier = varidSignature match {
        case Some(_) => val (t, q) = vds.queryVA("va.varid")
          t match {
            case TString => q
            case _ => a => null
          }
        case None => a => null
      }

      val rsidSignature = vds.vaSignature.getOption("rsid")
      val rsidQuery: Querier = rsidSignature match {
        case Some(_) => val (t, q) = vds.queryVA("va.rsid")
          t match {
            case TString => q
            case _ => a => null
          }
        case None => a => null
      }

      vds.rdd.mapPartitions { it: Iterator[(Variant, (Annotation, Iterable[Genotype]))] =>
        val sb = new StringBuilder
        it.map { case (v, (va, gs)) =>
          sb.clear()
          appendRow(sb, v, va, gs, rsidQuery, varidQuery)
          sb.result()
        }
      }.writeTable(path + ".gen", vds.hc.tmpDir, None)
    }

    writeSampleFile()
    writeGenFile()
  }

  def exportPlink(path: String, famExpr: String = "id = s") {
    require(vds.wasSplit)
    vds.requireSampleTString("export plink")

    val ec = EvalContext(Map(
      "s" -> (0, TString),
      "sa" -> (1, vds.saSignature),
      "global" -> (2, vds.globalSignature)))

    ec.set(2, vds.globalAnnotation)

    type Formatter = (Option[Any]) => String

    val formatID: Formatter = _.map(_.asInstanceOf[String]).getOrElse("0")
    val formatIsFemale: Formatter = _.map { a =>
      if (a.asInstanceOf[Boolean])
        "2"
      else
        "1"
    }.getOrElse("0")
    val formatIsCase: Formatter = _.map { a =>
      if (a.asInstanceOf[Boolean])
        "2"
      else
        "1"
    }.getOrElse("-9")
    val formatQPheno: Formatter = a => a.map(_.toString).getOrElse("-9")

    val famColumns: Map[String, (Type, Int, Formatter)] = Map(
      "famID" -> (TString, 0, formatID),
      "id" -> (TString, 1, formatID),
      "patID" -> (TString, 2, formatID),
      "matID" -> (TString, 3, formatID),
      "isFemale" -> (TBoolean, 4, formatIsFemale),
      "qPheno" -> (TFloat64, 5, formatQPheno),
      "isCase" -> (TBoolean, 5, formatIsCase))

    val (names, types, f) = Parser.parseNamedExprs(famExpr, ec)

    val famFns: Array[(Array[Option[Any]]) => String] = Array(
      _ => "0", _ => "0", _ => "0", _ => "0", _ => "-9", _ => "-9")

    (names.zipWithIndex, types).zipped.foreach { case ((name, i), t) =>
      famColumns.get(name) match {
        case Some((colt, j, formatter)) =>
          if (colt != t)
            fatal(s"invalid type for .fam file column $i: expected $colt, got $t")
          famFns(j) = (a: Array[Option[Any]]) => formatter(a(i))

        case None =>
          fatal(s"no .fam file column $name")
      }
    }

    val spaceRegex = """\s+""".r
    val badSampleIds = vds.stringSampleIds.filter(id => spaceRegex.findFirstIn(id).isDefined)
    if (badSampleIds.nonEmpty) {
      fatal(
        s"""Found ${ badSampleIds.length } sample IDs with whitespace
           |  Please run `renamesamples' to fix this problem before exporting to plink format
           |  Bad sample IDs: @1 """.stripMargin, badSampleIds)
    }

    val bedHeader = Array[Byte](108, 27, 1)

    val nSamples = vds.nSamples

    val plinkRDD = vds.rdd
      .mapValuesWithKey { case (v, (va, gs)) => ExportBedBimFam.makeBedRow(gs, nSamples) }
      .persist(StorageLevel.MEMORY_AND_DISK)

    plinkRDD.map { case (v, bed) => bed }
      .saveFromByteArrays(path + ".bed", vds.hc.tmpDir, header = Some(bedHeader))

    plinkRDD.map { case (v, bed) => ExportBedBimFam.makeBimRow(v) }
      .writeTable(path + ".bim", vds.hc.tmpDir)

    plinkRDD.unpersist()

    val famRows = vds
      .sampleIdsAndAnnotations
      .map { case (s, sa) =>
        ec.setAll(s, sa)
        val a = f().map(Option(_))
        famFns.map(_ (a)).mkString("\t")
      }

    vds.hc.hadoopConf.writeTextFile(path + ".fam")(out =>
      famRows.foreach(line => {
        out.write(line)
        out.write("\n")
      }))
  }

  /**
    *
    * @param filterExpr Filter expression involving v (variant), va (variant annotations), and aIndex (allele index)
    * @param annotationExpr Annotation modifying expression involving v (new variant), va (old variant annotations),
    * and aIndices (maps from new to old indices)
    * @param filterAlteredGenotypes any call that contains a filtered allele is set to missing instead
    * @param keep Keep variants matching condition
    * @param subset subsets the PL and AD. Genotype and GQ are set based on the resulting PLs.  Downcodes by default.
    * @param maxShift Maximum possible position change during minimum representation calculation
    */
  def filterAlleles(filterExpr: String, annotationExpr: String = "va = va", filterAlteredGenotypes: Boolean = false,
    keep: Boolean = true, subset: Boolean = true, maxShift: Int = 100, keepStar: Boolean = false): VariantDataset = {
    FilterAlleles(vds, filterExpr, annotationExpr, filterAlteredGenotypes, keep, subset, maxShift, keepStar)
  }

  def grm(): KinshipMatrix = {
    require(vds.wasSplit)
    info("Computing GRM...")
    GRM(vds)
  }

  def hardCalls(): VariantDataset = {
    vds.mapValues(TGenotype, { g =>
      if (g == null)
        g
      else
        Genotype(g._unboxedGT, g._fakeRef)
    })
  }

  /**
    *
    * @param computeMafExpr An expression for the minor allele frequency of the current variant, `v', given
    * the variant annotations `va'. If unspecified, MAF will be estimated from the dataset
    * @param bounded Allows the estimations for Z0, Z1, Z2, and PI_HAT to take on biologically-nonsense values
    * (e.g. outside of [0,1]).
    * @param minimum Sample pairs with a PI_HAT below this value will not be included in the output. Must be in [0,1]
    * @param maximum Sample pairs with a PI_HAT above this value will not be included in the output. Must be in [0,1]
    */
  def ibd(computeMafExpr: Option[String] = None, bounded: Boolean = true,
    minimum: Option[Double] = None, maximum: Option[Double] = None): KeyTable = {
    require(vds.wasSplit)
    IBD.toKeyTable(vds.hc, IBD.validateAndCall(vds, computeMafExpr, bounded, minimum, maximum))
  }

  /**
    *
    * @param mafThreshold Minimum minor allele frequency threshold
    * @param includePAR Include pseudoautosomal regions
    * @param fFemaleThreshold Samples are called females if F < femaleThreshold
    * @param fMaleThreshold Samples are called males if F > maleThreshold
    * @param popFreqExpr Use an annotation expression for estimate of MAF rather than computing from the data
    */
  def imputeSex(mafThreshold: Double = 0.0, includePAR: Boolean = false, fFemaleThreshold: Double = 0.2,
    fMaleThreshold: Double = 0.8, popFreqExpr: Option[String] = None): VariantDataset = {
    require(vds.wasSplit)

    val result = ImputeSexPlink(vds,
      mafThreshold,
      includePAR,
      fMaleThreshold,
      fFemaleThreshold,
      popFreqExpr)

    val signature = ImputeSexPlink.schema

    vds.annotateSamples(result, signature, "sa.imputesex")
  }

  def ldMatrix(forceLocal: Boolean = false): LDMatrix = {
    require(vds.wasSplit)
    LDMatrix(vds, Some(forceLocal))
  }

  def ldPrune(nCores: Int, r2Threshold: Double = 0.2, windowSize: Int = 1000000, memoryPerCore: Int = 256): VariantDataset = {
    require(vds.wasSplit)
    LDPrune(vds, nCores, r2Threshold, windowSize, memoryPerCore * 1024L * 1024L)
  }

  def mendelErrors(ped: Pedigree): (KeyTable, KeyTable, KeyTable, KeyTable) = {
    require(vds.wasSplit)
    vds.requireSampleTString("mendel errors")

    val men = MendelErrors(vds, ped.filterTo(vds.stringSampleIdSet).completeTrios)

    (men.mendelKT(), men.fMendelKT(), men.iMendelKT(), men.lMendelKT())
  }

  def nirvana(config: String, blockSize: Int = 500000, root: String): VariantDataset = {
    Nirvana.annotate(vds, config, blockSize, root)
  }

  /**
    *
    * @param scoresRoot Sample annotation path for scores (period-delimited path starting in 'sa')
    * @param k Number of principal components
    * @param loadingsRoot Variant annotation path for site loadings (period-delimited path starting in 'va')
    * @param eigenRoot Global annotation path for eigenvalues (period-delimited path starting in 'global'
    * @param asArrays Store score and loading results as arrays, rather than structs
    */
  def pca(scoresRoot: String, k: Int = 10, loadingsRoot: Option[String] = None, eigenRoot: Option[String] = None,
    asArrays: Boolean = false): VariantDataset = {
    require(vds.wasSplit)

    if (k < 1)
      fatal(
        s"""requested invalid number of components: $k
           |  Expect componenents >= 1""".stripMargin)

    info(s"Running PCA with $k components...")

    val pcSchema = SamplePCA.pcSchema(asArrays, k)

    val (scores, loadings, eigenvalues) =
      SamplePCA(vds, k, loadingsRoot.isDefined, eigenRoot.isDefined, asArrays)

    var ret = vds.annotateSamples(scores, pcSchema, scoresRoot)

    loadings.foreach { rdd =>
      ret = ret.annotateVariants(rdd.orderedRepartitionBy(vds.rdd.orderedPartitioner), pcSchema, loadingsRoot.get)
    }

    eigenvalues.foreach { eig =>
      ret = ret.annotateGlobal(eig, pcSchema, eigenRoot.get)
    }
    ret
  }

  /**
    *
    * @param k          the number of principal components to use to distinguish
    *                   ancestries
    * @param maf        the minimum individual-specific allele frequency for an
    *                   allele used to measure relatedness
    * @param blockSize  the side length of the blocks of the block-distributed
    *                   matrices; this should be set such that atleast three of
    *                   these matrices fit in memory (in addition to all other
    *                   objects necessary for Spark and Hail).
    * @param statistics which subset of the four statistics to compute
    */
  def pcRelate(k: Int, maf: Double, blockSize: Int, minKinship: Double = PCRelate.defaultMinKinship, statistics: PCRelate.StatisticSubset = PCRelate.defaultStatisticSubset): KeyTable = {
    require(vds.wasSplit)
    val pcs = SamplePCA.justScores(vds, k)
    PCRelate.toKeyTable(vds, pcs, maf, blockSize, minKinship, statistics)
  }

  def sampleQC(root: String = "sa.qc"): VariantDataset = SampleQC(vds.toGDS, root).toVDS

  def rrm(forceBlock: Boolean = false, forceGramian: Boolean = false): KinshipMatrix = {
    require(vds.wasSplit)
    info(s"rrm: Computing Realized Relationship Matrix...")
    val (rrm, m) = ComputeRRM(vds, forceBlock, forceGramian)
    info(s"rrm: RRM computed using $m variants.")
    KinshipMatrix(vds.hc, vds.sSignature, rrm, vds.sampleIds.toArray, m)
  }


  /**
    *
    * @param propagateGQ Propagate GQ instead of computing from PL
    * @param keepStar Do not filter * alleles
    * @param maxShift Maximum possible position change during minimum representation calculation
    */
  def splitMulti(propagateGQ: Boolean = false, keepStar: Boolean = false,
    maxShift: Int = 100): VariantDataset = {
    SplitMulti(vds, propagateGQ, keepStar, maxShift)
  }

  def tdt(ped: Pedigree, tdtRoot: String = "va.tdt"): VariantDataset = {
    require(vds.wasSplit)
    vds.requireSampleTString("TDT")
    TDT(vds, ped.filterTo(vds.stringSampleIdSet).completeTrios,
      Parser.parseAnnotationRoot(tdtRoot, Annotation.VARIANT_HEAD))
  }

  def variantQC(root: String = "va.qc"): VariantDataset = {
    require(vds.wasSplit)
    VariantQC(vds.toGDS, root).toVDS
  }

  def deNovo(ped: Pedigree,
    referenceAF: String,
    minGQ: Int = 20,
    minPDeNovo: Double = 0.05,
    maxParentAB: Double = 0.05,
    minChildAB: Double = 0.20,
    minDepthRatio: Double = 0.10): KeyTable = {
    require(vds.wasSplit)
    vds.requireSampleTString("de novo")

    DeNovo(vds, ped, referenceAF, minGQ, minPDeNovo, maxParentAB, minChildAB, minDepthRatio)
  }
}
