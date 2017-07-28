package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Shen on 2017/4/20.
  */
object MLDataSampler {
  def sample(
              sc: SparkContext,
              path: String,
              samplePercent: Double,
              path_new: String,
              n_partitions: Int
            ):
  Unit = {
    val data = sc.textFile(path)
    val sample_data = data.sample(false, samplePercent)

    val numFeatures = computeNumFeatures(sample_data)

    println(s"Number of features = $numFeatures")

    sample_data.coalesce(n_partitions).saveAsTextFile(path_new)
  }

  def computeNumFeatures(data: RDD[String]): Int = {
    data.map { s =>
      val n = s.split(' ').length
      s.split(' ')(n - 1).split(":")(0).toInt
    }.reduce(math.max)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SampleDataML")
    //      .setMaster("local")
    val sc = new SparkContext(conf)
    //    val path = "data/testLogistic"
    val path = args(0)
    //    val sparity = 0.3
    val samplePercent = args(1).toDouble
    //    val path_new = "data/testLogistic0.4"
    val path_new = args(2)
    val n_partitions = args(3).toInt
    sample(sc, path, samplePercent, path_new, n_partitions)
  }

}