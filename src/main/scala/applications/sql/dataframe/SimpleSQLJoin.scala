package applications.sql.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-21.
  *
  * SELECT * FROM Rankings As R, UserVisits As U
  * WHERE R.URL = U.URL;
  */
object SimpleSQLJoin {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: RDDJoinTest <table1_hdfs_file> <table2_hdfs_file> <output_file>")
      System.exit(1)
    }

    // $example on:init_session$
    val spark = SparkSession
      .builder()
      //.master("local[2]")
      //.config("spark.sql.shuffle.partitions", 32)
      .getOrCreate()

    // $example off:init_session$
    // $example on:programmatic_schema$
    // Create an RDD
    val rankings = spark.sparkContext.textFile(args(0))
    val uservisits = spark.sparkContext.textFile(args(1))



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

    val rankingsSchema = StructType(
      List(
        StructField("pageRank", IntegerType, true),
        StructField("pageURL", StringType, true),
        StructField("avgDuration", IntegerType, true)
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


    val rankingsRDD = rankings
      .map(_.split("\\|"))
      .map(attributes => Row(attributes(0).toInt, attributes(1), attributes(2).toInt))

    // Apply the schema to the RDD
    val rankingsDF = spark.createDataFrame(rankingsRDD, rankingsSchema)

    // Creates a temporary view using the DataFrame
    rankingsDF.createOrReplaceTempView("rankings")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT * FROM Rankings As R, UserVisits As U WHERE R.pageURL = U.destURL")

    println(results.rdd.toDebugString)
    println(results.explain())

    results.write.save(args(2))

  }

}

/*
(32) MapPartitionsRDD[25] at rdd at LocalSQLJoin.scala:91 []
 |   MapPartitionsRDD[24] at rdd at LocalSQLJoin.scala:91 []
 |   MapPartitionsRDD[23] at rdd at LocalSQLJoin.scala:91 []
 |   ZippedPartitionsRDD2[22] at rdd at LocalSQLJoin.scala:91 []
 |   MapPartitionsRDD[16] at rdd at LocalSQLJoin.scala:91 []
 |   ShuffledRowRDD[15] at rdd at LocalSQLJoin.scala:91 []
 +-(2) MapPartitionsRDD[14] at rdd at LocalSQLJoin.scala:91 []
    |  MapPartitionsRDD[13] at rdd at LocalSQLJoin.scala:91 []
    |  MapPartitionsRDD[12] at rdd at LocalSQLJoin.scala:91 []
    |  MapPartitionsRDD[10] at createDataFrame at LocalSQLJoin.scala:83 []
    |  MapPartitionsRDD[9] at map at LocalSQLJoin.scala:80 []
    |  MapPartitionsRDD[8] at map at LocalSQLJoin.scala:79 []
    |  /Users/xulijie/Documents/data/SQLdata/Rankings-100.txt MapPartitionsRDD[3] at textFile at LocalSQLJoin.scala:39 []
    |  /Users/xulijie/Documents/data/SQLdata/Rankings-100.txt HadoopRDD[2] at textFile at LocalSQLJoin.scala:39 []
 |   MapPartitionsRDD[21] at rdd at LocalSQLJoin.scala:91 []
 |   ShuffledRowRDD[20] at rdd at LocalSQLJoin.scala:91 []
 +-(2) MapPartitionsRDD[19] at rdd at LocalSQLJoin.scala:91 []
    |  MapPartitionsRDD[18] at rdd at LocalSQLJoin.scala:91 []
    |  MapPartitionsRDD[17] at rdd at LocalSQLJoin.scala:91 []
    |  MapPartitionsRDD[6] at createDataFrame at LocalSQLJoin.scala:72 []
    |  MapPartitionsRDD[5] at map at LocalSQLJoin.scala:68 []
    |  MapPartitionsRDD[4] at map at LocalSQLJoin.scala:67 []
    |  /Users/xulijie/Documents/data/SQLdata/UserVisits-100.txt MapPartitionsRDD[1] at textFile at LocalSQLJoin.scala:38 []
    |  /Users/xulijie/Documents/data/SQLdata/UserVisits-100.txt HadoopRDD[0] at textFile at LocalSQLJoin.scala:38 []
== Physical Plan ==
*SortMergeJoin [pageURL#33], [destURL#10], Inner
:- *Sort [pageURL#33 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(pageURL#33, 32)
:     +- *Filter isnotnull(pageURL#33)
:        +- Scan ExistingRDD[pageRank#32,pageURL#33,avgDuration#34]
+- *Sort [destURL#10 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(destURL#10, 32)
      +- *Filter isnotnull(destURL#10)
         +- Scan ExistingRDD[sourceIP#9,destURL#10,visitDate#11,adRevenue#12,userAgent#13,countryCode#14,languageCode#15,searchWord#16,duration#17]
()

 */
