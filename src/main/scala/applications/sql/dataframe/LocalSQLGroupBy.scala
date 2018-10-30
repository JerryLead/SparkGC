package applications.sql.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-21.
  *
  * SELECT sourceIP, visitDate, SUM(adRevenue) FROM uservisits GROUP BY sourceIP, visitDate;
  */
object LocalSQLGroupBy {
  def main(args: Array[String]): Unit = {

    val uservisitsPath = "/Users/xulijie/Documents/data/SQLdata/hibench/uservisits"

    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 32)
      .getOrCreate()


    // $example off:init_session$
    // $example on:programmatic_schema$
    // Create an RDD

    val uservisits = spark.sparkContext.textFile(uservisitsPath)

    // Generate the schema based on the string of schema
    val uservisitsSchema = StructType(
      List(
        StructField("sourceIP", StringType, true),
        StructField("destURL", StringType, true),
        StructField("d1", StringType, true),
        StructField("d2", StringType, true),
        StructField("visitDate", StringType, true),
        StructField("adRevenue", DoubleType, true),
        StructField("userAgent", StringType, true),
        StructField("countryCode", StringType, true),
        StructField("languageCode", StringType, true),
        StructField("searchWord", StringType, true),
        StructField("duration", IntegerType, true)
      )
    )

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\||\\t"))
      .map(attributes => Row(attributes(1), attributes(2), attributes(3), attributes(4), attributes(5),
        attributes(6).toDouble, attributes(7), attributes(8), attributes(9), attributes(10), attributes(11).toInt))

    // Apply the schema to the RDD
    val uservisitsDF = spark.createDataFrame(uservisitsRDD, uservisitsSchema)

    // Creates a temporary view using the DataFrame
    uservisitsDF.createOrReplaceTempView("uservisits")


    // SQL can be run over a temporary view created using DataFrames
    //val results = spark.sql("SELECT sourceIP, SUM(adRevenue) AS SUM " +
    //  "FROM uservisits GROUP BY sourceIP")

    val results = spark.sql("SELECT sourceIP, visitDate, SUM(adRevenue) AS SUM " +
      "FROM uservisits GROUP BY sourceIP, visitDate")

    println(results.rdd.toDebugString)
    println(results.explain())

    results.show()

  }

}

/*
+---------------+----------+-----------+
|       sourceIP| visitDate|        SUM|
+---------------+----------+-----------+
| 224.98.214.109|1981-08-08| 0.19020218|
|189.231.126.117|2004-03-19| 0.42735237|
|  158.153.26.27|1980-11-01| 0.24133557|
| 242.180.187.37|1977-12-29|  0.4665752|
|194.132.207.206|1975-07-02|  0.6696159|
| 167.223.155.38|1970-10-12|  0.9592076|
|   69.44.74.217|1992-02-05|  0.8155089|
|  36.184.32.190|1981-09-03| 0.85915947|
|  67.52.144.135|1972-07-20|  0.5670332|
| 171.186.74.183|1973-03-04| 0.67914915|
|   232.16.66.31|2002-09-02|  0.7311704|
| 232.249.52.100|1977-05-09| 0.99863476|
| 227.209.164.46|1991-06-10|0.115967035|
|  187.63.80.159|2000-03-03|  0.1102736|
|   34.57.45.175|2001-06-29| 0.14202267|
|   58.66.122.55|2010-03-17| 0.52077913|
| 46.160.116.124|2008-07-03|  0.5076599|
| 208.53.133.145|1981-05-05|  0.9478478|
|  55.193.166.11|2007-04-02|0.060459852|
| 108.158.25.195|1974-02-05|  0.9408617|
+---------------+----------+-----------+

(32) MapPartitionsRDD[12] at rdd at LocalSQLGroupBy.scala:68 []
 |   MapPartitionsRDD[11] at rdd at LocalSQLGroupBy.scala:68 []
 |   MapPartitionsRDD[10] at rdd at LocalSQLGroupBy.scala:68 []
 |   ShuffledRowRDD[9] at rdd at LocalSQLGroupBy.scala:68 []
 +-(2) MapPartitionsRDD[8] at rdd at LocalSQLGroupBy.scala:68 []
    |  MapPartitionsRDD[7] at rdd at LocalSQLGroupBy.scala:68 []
    |  MapPartitionsRDD[6] at rdd at LocalSQLGroupBy.scala:68 []
    |  MapPartitionsRDD[4] at createDataFrame at LocalSQLGroupBy.scala:55 []
    |  MapPartitionsRDD[3] at map at LocalSQLGroupBy.scala:51 []
    |  MapPartitionsRDD[2] at map at LocalSQLGroupBy.scala:50 []
    |  /Users/xulijie/Documents/data/SQLdata/hibench/uservisits MapPartitionsRDD[1] at textFile at LocalSQLGroupBy.scala:29 []
    |  /Users/xulijie/Documents/data/SQLdata/hibench/uservisits HadoopRDD[0] at textFile at LocalSQLGroupBy.scala:29 []
== Physical Plan ==
*HashAggregate(keys=[sourceIP#11, visitDate#15], functions=[sum(adRevenue#16)])
+- Exchange hashpartitioning(sourceIP#11, visitDate#15, 32)
   +- *HashAggregate(keys=[sourceIP#11, visitDate#15], functions=[partial_sum(adRevenue#16)])
      +- *Project [sourceIP#11, visitDate#15, adRevenue#16]
         +- Scan ExistingRDD[sourceIP#11,destURL#12,d1#13,d2#14,visitDate#15,adRevenue#16,userAgent#17,countryCode#18,languageCode#19,searchWord#20,duration#21]
()

 */
