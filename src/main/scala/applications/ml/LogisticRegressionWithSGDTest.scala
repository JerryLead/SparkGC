package applications.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by xulijie on 17-7-27.
  */
object LogisticRegressionWithSGDTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "sampledata/mllib/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 10
    val model = LogisticRegressionWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    sc.stop()
  }
}
