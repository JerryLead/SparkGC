import org.apache.spark.SparkContext

/**
  * Created by xulijie on 17-12-7.
  */
object SumTest {

  def main(args: Array[String]): Unit = {


    val sc = new SparkContext("local", "FlatMap Test")
    val data = Array[(String, Int)](("A", 1), ("B", 2),
      ("B", 3), ("C", 4),
      ("C", 5), ("C", 6)
    )
    val pairs = sc.makeRDD(data, 3)

    val result = pairs.map(x => x._2).sum()

    println(result)
  }
}
