import org.apache.spark.sql.SparkSession

/**
  * Created by xulijie on 18-1-22.
  */
object GroupByExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[2]")
      .getOrCreate()
    val d = spark.sparkContext.parallelize(1 to 100, 10)

    val pairs = d.keyBy(x => x % 10)

    val result1 = pairs.groupByKey()
    //val result2 = pairs.groupByKey(3)
    //val result3 = pairs.groupByKey(new RangePartitioner(3, pairs))

    println("Result 1:")
    result1.foreach(println)
    println(result1.toDebugString)

    //println("Result 2:")
    //result2.foreach(println)

    //println("Result 3:")
    //result3.foreach(println)

  }

}
