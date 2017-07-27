package applications.ml

/**
  * Created by xulijie on 17-6-20.
  */

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils


object SVMWithSGDExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      // .setAppName("SVM").setMaster("local[2]")
    val sc = new SparkContext(conf)

    if (args.length < 3) {
      System.err.println("Usage: SVMWithSGDExample <file> <iter> <dimension>")
      System.exit(1)
    }

    val path = args(0)
    val numIterations = args(1).toInt
    val numFeatures = args(2).toInt


    // val path = "sampledata/mllib/kdd12-sample.txt"
    // val path = "sampledata/mllib/sample_libsvm_data.txt"

    // specify the numFeatures to avoid caching this data
    val data = MLUtils.loadLibSVMFile(sc, path, numFeatures)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = SVMWithSGD.train(training, numIterations)

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

    val weights = model.weights
    println(s"Size of wights = " + weights.size)
    println("Area under ROC = " + auROC)

    sc.stop()
  }
}