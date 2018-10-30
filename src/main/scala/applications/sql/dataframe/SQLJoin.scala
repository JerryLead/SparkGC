package applications.sql.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-21.
  *
  * SELECT URL, adRevenue, pageRank FROM Rankings As R, UserVisits As UV WHERE R.URL = UV.URL
  */
object SQLJoin {
  def main(args: Array[String]): Unit = {


    if (args.length < 3) {
      System.err.println("Usage: SQLJoin <table1_hdfs_file> <table2_hdfs_file> <output_file>")
      System.exit(1)
    }


    val uservisitsPath = args(0)
    val rankingsPath = args(1)

    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .getOrCreate()

    // $example off:init_session$
    // $example on:programmatic_schema$
    // Create an RDD

    val uservisits = spark.sparkContext.textFile(uservisitsPath)
    val rankings = spark.sparkContext.textFile(rankingsPath)


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

    val rankingsSchema = StructType(
      List(
        StructField("pageURL", StringType, true),
        StructField("pageRank", IntegerType, true),
        StructField("avgDuration", IntegerType, true)
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


    val rankingsRDD = rankings
      .map(_.split("\\||\\t"))
      .map(attributes => Row(attributes(1), attributes(2).toInt, attributes(3).toInt))

    // Apply the schema to the RDD
    val rankingsDF = spark.createDataFrame(rankingsRDD, rankingsSchema)

    // Creates a temporary view using the DataFrame
    rankingsDF.createOrReplaceTempView("rankings")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT pageURL, adRevenue, pageRank FROM Rankings As R, UserVisits As UV WHERE R.pageURL = UV.destURL")

    println(results.rdd.toDebugString)
    println(results.explain())

    results.write.save(args(2))
  }

}

/*
+--------------------+-----------+--------+
|             pageURL|  adRevenue|pageRank|
+--------------------+-----------+--------+
|  cxdmunpixtrqnvglnt|  0.9158855|    4194|
|  cxdmunpixtrqnvglnt| 0.30648273|    4194|
|  cxdmunpixtrqnvglnt| 0.24133557|    4194|
|  cxdmunpixtrqnvglnt|  0.1102736|    4194|
|  cxdmunpixtrqnvglnt| 0.47234488|    4194|
|  cxdmunpixtrqnvglnt|  0.5127188|    4194|
|tpmltuwalvkduavbw...|  0.7072385|    2443|
|tpmltuwalvkduavbw...| 0.45038307|    2443|
|tpmltuwalvkduavbw...| 0.42592716|    2443|
|tpmltuwalvkduavbw...|  0.8941969|    2443|
|mtuqcwwotyhumzeeb...|  0.2751385|    2620|
|mtuqcwwotyhumzeeb...| 0.48823595|    2620|
|mtuqcwwotyhumzeeb...| 0.16307545|    2620|
|mtuqcwwotyhumzeeb...|  0.4379751|    2620|
|mtuqcwwotyhumzeeb...|  0.9592076|    2620|
|mtuqcwwotyhumzeeb...|  0.9662546|    2620|
|mtuqcwwotyhumzeeb...|0.061952174|    2620|
|mtuqcwwotyhumzeeb...| 0.36095577|    2620|
|mtuqcwwotyhumzeeb...| 0.12911129|    2620|
|nirxfdnojzdnpmsmi...|  0.3493713|    2179|
+--------------------+-----------+--------+
only showing top 20 rows

(32) MapPartitionsRDD[25] at rdd at LocalSQLJoin.scala:92 []
 |   MapPartitionsRDD[24] at rdd at LocalSQLJoin.scala:92 []
 |   MapPartitionsRDD[23] at rdd at LocalSQLJoin.scala:92 []
 |   ZippedPartitionsRDD2[22] at rdd at LocalSQLJoin.scala:92 []
 |   MapPartitionsRDD[16] at rdd at LocalSQLJoin.scala:92 []
 |   ShuffledRowRDD[15] at rdd at LocalSQLJoin.scala:92 []
 +-(2) MapPartitionsRDD[14] at rdd at LocalSQLJoin.scala:92 []
    |  MapPartitionsRDD[13] at rdd at LocalSQLJoin.scala:92 []
    |  MapPartitionsRDD[12] at rdd at LocalSQLJoin.scala:92 []
    |  MapPartitionsRDD[10] at createDataFrame at LocalSQLJoin.scala:84 []
    |  MapPartitionsRDD[9] at map at LocalSQLJoin.scala:81 []
    |  MapPartitionsRDD[8] at map at LocalSQLJoin.scala:80 []
    |  /Users/xulijie/Documents/data/SQLdata/hibench/rankings MapPartitionsRDD[3] at textFile at LocalSQLJoin.scala:38 []
    |  /Users/xulijie/Documents/data/SQLdata/hibench/rankings HadoopRDD[2] at textFile at LocalSQLJoin.scala:38 []
 |   MapPartitionsRDD[21] at rdd at LocalSQLJoin.scala:92 []
 |   ShuffledRowRDD[20] at rdd at LocalSQLJoin.scala:92 []
 +-(2) MapPartitionsRDD[19] at rdd at LocalSQLJoin.scala:92 []
    |  MapPartitionsRDD[18] at rdd at LocalSQLJoin.scala:92 []
    |  MapPartitionsRDD[17] at rdd at LocalSQLJoin.scala:92 []
    |  MapPartitionsRDD[6] at createDataFrame at LocalSQLJoin.scala:73 []
    |  MapPartitionsRDD[5] at map at LocalSQLJoin.scala:69 []
    |  MapPartitionsRDD[4] at map at LocalSQLJoin.scala:68 []
    |  /Users/xulijie/Documents/data/SQLdata/hibench/uservisits MapPartitionsRDD[1] at textFile at LocalSQLJoin.scala:37 []
    |  /Users/xulijie/Documents/data/SQLdata/hibench/uservisits HadoopRDD[0] at textFile at LocalSQLJoin.scala:37 []
== Physical Plan ==
*Project [pageURL#38, adRevenue#16, pageRank#39]
+- *SortMergeJoin [pageURL#38], [destURL#12], Inner
   :- *Sort [pageURL#38 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(pageURL#38, 32)
   :     +- *Project [pageURL#38, pageRank#39]
   :        +- *Filter isnotnull(pageURL#38)
   :           +- Scan ExistingRDD[pageURL#38,pageRank#39,avgDuration#40]
   +- *Sort [destURL#12 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(destURL#12, 32)
         +- *Project [destURL#12, adRevenue#16]
            +- *Filter isnotnull(destURL#12)
               +- Scan ExistingRDD[sourceIP#11,destURL#12,d1#13,d2#14,visitDate#15,adRevenue#16,userAgent#17,countryCode#18,languageCode#19,searchWord#20,duration#21]
()
 */
