package org.broadinstitute.hail.stats

import breeze.stats.distributions._
import breeze.linalg.{DenseVector, sum}
import org.apache.spark.SparkContext
import org.apache.commons.math3.random.JDKRandomGenerator
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr.{TArray, TDouble, TInt, TStruct}
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.variant.{Genotype, Variant, VariantDataset, VariantMetadata}

/**
  * Generates a VariantDataSet based on Balding Nichols Model
  */
object BaldingNicholsModelDist {
  // K populations, N samples, M variants
  // popDist is K-vector proportional to population distribution
  // FstOfPop is K-vector of F_st values
  def apply(sc: SparkContext, K: Int, N: Int, M: Int,
    popDist: DenseVector[Double],
    FstOfPop: DenseVector[Double],
    seed: Int): VariantDataset = {

    require(K > 0)
    require(N > 0)
    require(M > 0)
    require(popDist.length == K)
    require(popDist.forall(_ >= 0d))
    require(FstOfPop.length == K)
    require(FstOfPop.forall(f => f > 0d && f < 1d))

    info(s"baldingnichols: generating genotypes for $K populations, $N samples, and $M variants...")

    Rand.generator.setSeed(seed)

    val popDist_k = popDist
    popDist_k :/= sum(popDist_k)

    val popDistRV = Multinomial(popDist_k)
    val popOfSample_n: DenseVector[Int] = DenseVector.fill[Int](N)(popDistRV.draw())
    val popOfSample_nBc = sc.broadcast(popOfSample_n)

    val Fst_k = FstOfPop
    val Fst1_k = (1d - Fst_k) :/ Fst_k
    val Fst1_kBc = sc.broadcast(Fst1_k)

    val variantSeed = Rand.randInt.draw();
    val variantSeedBc = sc.broadcast(variantSeed)

    val numPartitions = Math.max(N * M / 1000000, 8)

    val rdd = sc.parallelize(
      (0 until M).map { m =>
        val perVariantRandomGenerator = new JDKRandomGenerator
        perVariantRandomGenerator.setSeed(variantSeedBc.value + m)
        val perVariantRandomBasis = new RandBasis(perVariantRandomGenerator)

        val unif = new Uniform(0, 1)(perVariantRandomBasis)

        val ancestralAF = Uniform(.1, .9).draw()

        val popAF_k = (0 until K).map{k =>
          new Beta((1 - ancestralAF) * Fst1_kBc.value(k), ancestralAF * Fst1_kBc.value(k)).draw()
        }

        (Variant("1", m + 1, "A", "C"),
          (Annotation(ancestralAF, popAF_k),
            (0 until N).map { n =>
              val p = popAF_k(popOfSample_nBc.value(n))
              val pSq = p * p
              val x = unif.draw()
              val genotype_num =
                if (x < pSq)
                  0
                else if (x > 2 * p - pSq)
                  2
                else
                  1
              Genotype(genotype_num)
            }: Iterable[Genotype]
          )
        )
      },
      numPartitions
    ).toOrderedRDD

    val sampleIds = (1 to N).map(_.toString).toArray
    val sampleAnnotations = popOfSample_n.toArray: IndexedSeq[Int]
    val globalAnnotation = Annotation(K, N, M, popDist_k.toArray: IndexedSeq[Double], Fst_k.toArray: IndexedSeq[Double], seed)
    val saSignature = TStruct("pop" -> TInt)
    val vaSignature = TStruct("ancestralAF" -> TDouble, "AF" -> TArray(TDouble))
    val globalSignature = TStruct("nPops" -> TInt, "nSamples" -> TInt, "nVariants" -> TInt,
      "popDist" -> TArray(TDouble), "Fst" -> TArray(TDouble), "seed" -> TInt)
    val wasSplit = true
    val isDosage = false

    new VariantDataset(new VariantMetadata(sampleIds, sampleAnnotations, globalAnnotation, saSignature,
      vaSignature, globalSignature, wasSplit, isDosage), rdd)


  }
}
