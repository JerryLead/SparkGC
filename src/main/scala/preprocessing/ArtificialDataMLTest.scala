package preprocessing

import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by Shen on 2017/3/8.
  * cate: classify, regression, logistic, SVM, cluster
  */
object ArtificialDataMLTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ArtificiaDataML")
    //      .setMaster("local")
    // partition is defined with conf: spark.default.parallelism
    val sc = new SparkContext(conf)
    val path = args(0)
    val samples = args(1).toInt
    val features = args(2).toInt
    val cate = args(3)
    val numPartition = args(4).toInt
    ArtificiaDataML.run(sc, samples, features, cate, path, numPartition)
    sc.stop()

  }
}
