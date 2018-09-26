package applications.sql.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-21.
  *
  * SELECT SUBSTR(sourceIP, 1, 7), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 7);
  */
object LocalSQLGroupBy {
  def main(args: Array[String]): Unit = {

    val uservisitsPath = "/Users/xulijie/Documents/data/SQLdata/UserVisits-100.txt"

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
        StructField("visitDate", StringType, true),
        StructField("adRevenue", FloatType, true),
        StructField("userAgent", StringType, true),
        StructField("countryCode", StringType, true),
        StructField("languageCode", StringType, true),
        StructField("searchWord", StringType, true),
        StructField("duration", IntegerType, true)
      )
    )

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\|"))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3).toFloat, attributes(4), attributes(5),
        attributes(6), attributes(7), attributes(8).toInt))

    // Apply the schema to the RDD
    val uservisitsDF = spark.createDataFrame(uservisitsRDD, uservisitsSchema)

    // Creates a temporary view using the DataFrame
    uservisitsDF.createOrReplaceTempView("uservisits")


    // SQL can be run over a temporary view created using DataFrames
    //val results = spark.sql("SELECT sourceIP, SUM(adRevenue) AS SUM " +
    //  "FROM uservisits GROUP BY sourceIP")

    val results = spark.sql("SELECT sourceIP, SUM(adRevenue) AS SUM " +
      "FROM uservisits GROUP BY sourceIP")

    println(results.rdd.toDebugString)
    println(results.explain())

    results.show()

  }

}

/*
+-------------------------+------------------+
|substring(sourceIP, 1, 7)|    sum(adRevenue)|
+-------------------------+------------------+
|                  165.133|   215.73193359375|
|                  159.113|189.19805908203125|
|                  169.112|288.46917724609375|
|                  159.128| 87.09142303466797|
|                  175.100|253.34176635742188|
|                  176.121|346.56024169921875|
|                  169.107| 357.6817932128906|
|                  156.134| 295.5165710449219|
|                  167.126| 345.7102355957031|
|                  173.110| 287.6191101074219|
|                  165.124|103.69808959960938|
|                  165.100|171.85166931152344|
|                  172.113|383.46649169921875|
|                  173.126| 359.8050842285156|
|                  155.100|150.10218811035156|
|                  161.115|268.07354736328125|
|                  175.129| 97.90043640136719|
|                  167.110| 311.0115051269531|
|                  177.127|256.38543701171875|
|                  168.129|368.29364013671875|
+-------------------------+------------------+

(200) MapPartitionsRDD[12] at rdd at SQLGroupBy.scala:73 []
  |   MapPartitionsRDD[11] at rdd at SQLGroupBy.scala:73 []
  |   MapPartitionsRDD[10] at rdd at SQLGroupBy.scala:73 []
  |   ShuffledRowRDD[9] at rdd at SQLGroupBy.scala:73 []
  +-(2) MapPartitionsRDD[8] at rdd at SQLGroupBy.scala:73 []
     |  MapPartitionsRDD[7] at rdd at SQLGroupBy.scala:73 []
     |  MapPartitionsRDD[6] at rdd at SQLGroupBy.scala:73 []
     |  MapPartitionsRDD[4] at createDataFrame at SQLGroupBy.scala:62 []
     |  MapPartitionsRDD[3] at map at SQLGroupBy.scala:58 []
     |  MapPartitionsRDD[2] at map at SQLGroupBy.scala:57 []
     |  /Users/xulijie/Documents/data/SQLdata/UserVisits-100.txt MapPartitionsRDD[1] at textFile at SQLGroupBy.scala:34 []
     |  /Users/xulijie/Documents/data/SQLdata/UserVisits-100.txt HadoopRDD[0] at textFile at SQLGroupBy.scala:34 []

== Physical Plan ==
*HashAggregate(keys=[substring(sourceIP#9, 1, 7)#50], functions=[sum(cast(adRevenue#12 as double))])
+- Exchange hashpartitioning(substring(sourceIP#9, 1, 7)#50, 200)
   +- *HashAggregate(keys=[substring(sourceIP#9, 1, 7) AS substring(sourceIP#9, 1, 7)#50], functions=[partial_sum(cast(adRevenue#12 as double))])
      +- *Project [sourceIP#9, adRevenue#12]
         +- Scan ExistingRDD[sourceIP#9,destURL#10,visitDate#11,adRevenue#12,userAgent#13,countryCode#14,languageCode#15,searchWord#16,duration#17]


 */
