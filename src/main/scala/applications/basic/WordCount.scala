package applications.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xulijie on 17-6-20.
  */

object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val outputFile = args(1)

    val textFile = sc.textFile(inputFile)
    val result = textFile.flatMap(_.split("[ |\\.]"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    result.saveAsTextFile(outputFile)

    sc.stop()
  }
}
