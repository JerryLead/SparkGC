package applications.ml

/**
  * Created by xulijie on 17-6-20.
  */

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
// $example off$

object SVMWithSGDExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //      .setAppName("SVMWithSGDExample")
    val sc = new SparkContext(conf)
    // $example on$
    // Load training data in LIBSVM format.
    //    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    //    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    //    val training = splits(0).cache()
    //    val test = splits(1)

    val path = args(0)

    val training = MLUtils.loadLibSVMFile(sc, path, 27343227)
    //    val test = MLUtils.loadLibSVMFile(sc, path_test)
    val test = training.sample(false, 0.2)
    // Run training algorithm to build the model
    val numIterations = 10
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

    // Save and load model
    //    model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    //    val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
    // $example off$

    sc.stop()
  }
}