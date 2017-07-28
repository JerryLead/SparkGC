package preprocessing


import breeze.numerics.sin
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

import Array._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created by Shen on 2017/3/8.
  *
  * generate libSVMFormat data for machine learning of classification or regression
  * and store the data on HDFS.
  * the generation approach is refer to "Artificial Data Sets" in paper
  * "Fast Cross-Validation via Sequential Testing".
  */
class ArtificialDataML() {

  /**
    * Generate traning data set for classification.
    * local:
    * get random x in [0, 2pi],
    * generate a list M = [1 to d],
    * for each element m in M, calculate y = sin(x+2pi*m)+ beta,
    * beta is a random value obey Gaussian distribution (0, 0.25**2),
    * the value 0.25 can value from 0.25 to 0.5, but was assigned 0.25 temporarily.
    * then get the list Y and add a label 1.0 if sin(x) > 0 else 0.0
    *
    **/
  def dataForClassify(
                       sc: SparkContext,
                       numSample: Int,
                       numFeature: Int,
                       numPartition: Int
                     ): RDD[LabeledPoint] = {
    val rand = new Random()
    val labels = RandomRDDs.uniformRDD(sc, numSample, numPartition)
    labels.map(x => {
      val label = {
        if (sin(x * 2 * Math.PI) > 0) {
          1.0
        }
        else {
          0.0
        }
      }
      val seq = {
        val array = range(1, numFeature + 1)
        array.map(a => (a - 1, sin((x + a) * 2 * Math.PI) + 0.25 * rand.nextGaussian()))
      }
      LabeledPoint(label, Vectors.sparse(numFeature + 1, seq))
    }
    )
  }

  /**
    * Generate traning data set for regression.
    * local:
    * get random x in [-pi, pi],
    * label = sin(4x)/(4x)
    * generate a list M = [1 to d],
    * for each element m in M, return y = sin(4x)/(4x) + sin(15mx)/5 + beta,
    * beta is a random value obey Gaussian distribution (0, 0.1**2),
    * the value 0.1 can value from 0.1 to 0.2, but was assigned 0.1 temporarily,
    * then get the list Y and add the label.
    *
    **/
  def dataForRegression(
                         sc: SparkContext,
                         numSample: Int,
                         numFeature: Int,
                         numPartition: Int
                       ): RDD[LabeledPoint] = {
    val rand = new Random()
    val labels = RandomRDDs.uniformRDD(sc, numSample, numPartition)
    labels.map(x => {
      val pix = x * 2 * Math.PI - Math.PI

      val label = {
        sin(4 * pix) / (4 * pix)
      }
      val seq = {
        val array = range(1, numFeature + 1)
        array.map(a => (a - 1, label + sin(15 * a * pix) / 5 + 0.1 * rand.nextGaussian()))
      }
      LabeledPoint(label, Vectors.sparse(numFeature + 1, seq))
    }
    )
  }

  def generateLogisticRDD(
                           sc: SparkContext,
                           nexamples: Int,
                           nfeatures: Int,
                           eps: Double,
                           nparts: Int = 8,
                           probOne: Double = 0.5): RDD[LabeledPoint] = {
    val data = sc.parallelize(0 until nexamples, nparts).map { idx =>
      val rnd = new Random(42 + idx)

      val y = if (idx % 2 == 0) 0.0 else 1.0
      val x = Array.fill[Double](nfeatures) {
        rnd.nextGaussian() + (y * eps)
      }
      LabeledPoint(y, Vectors.dense(x))
    }
    data
  }

  def generateSVMRDD(
                      sc: SparkContext,
                      nexamples: Int,
                      nfeatures: Int,
                      parts: Int = 8
                    ): RDD[LabeledPoint] = {
    val globalRnd = new Random(94720)
    val trueWeights = Array.fill[Double](nfeatures)(globalRnd.nextGaussian())

    val data: RDD[LabeledPoint] = sc.parallelize(0 until nexamples, parts).map { idx =>
      val rnd = new Random(42 + idx)

      val x = Array.fill[Double](nfeatures) {
        rnd.nextDouble() * 2.0 - 1.0
      }
      val yD = blas.ddot(trueWeights.length, x, 1, trueWeights, 1) + rnd.nextGaussian() * 0.1
      val y = if (yD < 0) 0.0 else 1.0
      LabeledPoint(y, Vectors.dense(x))
    }
    data
  }

  /**
    * Generate an RDD containing test data for KMeans.
    *
    * @param sc            SparkContext to use for creating the RDD
    * @param numPoints     Number of points that will be contained in the RDD
    * @param k             Number of clusters
    * @param d             Number of dimensions
    * @param r             Scaling factor for the distribution of the initial centers
    * @param numPartitions Number of partitions of the generated RDD; default 2
    */
  def generateKMeansRDD(
                         sc: SparkContext,
                         numPoints: Int,
                         k: Int,
                         d: Int,
                         r: Double,
                         numPartitions: Int = 8)
  : RDD[Array[Double]] = {
    // First, generate some centers
    val rand = new Random(42)
    val centers = Array.fill(k)(Array.fill(d)(rand.nextGaussian() * r))
    // Then generate points around each center
    sc.parallelize(0 until numPoints, numPartitions).map { idx =>
      val center = centers(idx % k)
      val rand2 = new Random(42 + idx)
      Array.tabulate(d)(i => center(i) + rand2.nextGaussian())
    }
  }

  /**
    * TODO
    *
    **/
  def dataForCluster(): Unit = {


  }

  /**
    * TODO
    *
    **/
  def dataForALS(): Unit = {

  }
}

object ArtificiaDataML {
  def run(
           sc: SparkContext,
           numSample: Int,
           numFeature: Int,
           algorithm: String,
           path: String,
           numPartition: Int
         ) {
    val gen = new ArtificialDataML();

    algorithm match {
      case "classify" => {
        val pairs = gen.dataForClassify(sc, numSample, numFeature, numPartition);
        MLUtils.saveAsLibSVMFile(pairs, path)
      }

      case "regression" => {
        val pairs = gen.dataForRegression(sc, numSample, numFeature, numPartition);
        MLUtils.saveAsLibSVMFile(pairs, path)
      }

      case "logistic" => {
        val pairs = gen.generateLogisticRDD(sc, numSample, numFeature, 3, numPartition)
        MLUtils.saveAsLibSVMFile(pairs, path)
      }

      case "SVM" => {
        val pairs = gen.generateSVMRDD(sc, numSample, numFeature, numPartition)
        MLUtils.saveAsLibSVMFile(pairs, path)
      }

      case "cluster" => {
        val pairs = gen.generateKMeansRDD(sc, numSample, 8, numFeature, 1.5, numPartition)
        //          val pairs = gen.generateKMeansRDD(sc, numSample, 8, numFeature, 1.0, numPartition)
        pairs.map(_.mkString(" ")).saveAsTextFile(path)
      }

    }
  }
}