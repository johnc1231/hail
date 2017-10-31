package is.hail.stats

import is.hail.annotations.Annotation
import is.hail.expr.{Field, TFloat64, TInt32, TStruct}
import is.hail.utils._
import is.hail.variant.Genotype

object InfoScoreCombiner {
  def signature = TStruct(Array(
    ("score", TFloat64, "IMPUTE info score"),
    ("nIncluded", TInt32, "Number of samples with non-missing genotype probability distribution")
  ).zipWithIndex.map { case ((n, t, d), i) => Field(n, t, i, Map(("desc", d))) })
}

class InfoScoreCombiner extends Serializable {
  var result = 0d
  var expectedAlleleCount = 0d
  var totalDosage = 0d
  var nIncluded = 0

  def expectedVariance(gp: IndexedSeq[Double], mean: Double): Double = (gp(1) + 4 * gp(2)) - (mean * mean)

  def merge(gp: IndexedSeq[Double]): InfoScoreCombiner = {
    if (gp != null) {
      val mean = gp(1) + 2 * gp(2)
      result += expectedVariance(gp, mean)
      expectedAlleleCount += mean
      totalDosage += gp.sum
      nIncluded += 1
    }
    this
  }

  def merge(that: InfoScoreCombiner): InfoScoreCombiner = {
    result += that.result
    expectedAlleleCount += that.expectedAlleleCount
    totalDosage += that.totalDosage
    nIncluded += that.nIncluded
    this
  }

  def thetaMLE: Option[Double] = divOption(expectedAlleleCount, totalDosage)

  def imputeInfoScore(theta: Double): Option[Double] =
    if (theta == 1.0 || theta == 0.0)
      Some(1d)
    else if (nIncluded == 0)
      None
    else
      Some(1d - ((result / nIncluded) / (2 * theta * (1 - theta))))

  def asAnnotation: Annotation = {
    val score = thetaMLE.flatMap { theta => imputeInfoScore(theta) }

    Annotation(score.orNull, nIncluded)
  }
}