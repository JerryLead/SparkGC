package applications.sql.rdd

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-20.
  *
  * SELECT URL, adRevenue, pageRank FROM Rankings As R, UserVisits As UV WHERE R.URL = UR.URL
  */
object LocalRDDJoin {
  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()


    val uservisitsPath = "/Users/xulijie/Documents/data/SQLdata/hibench/uservisits"
    val rankingsPath = "/Users/xulijie/Documents/data/SQLdata/hibench/rankings"

    // $example off:init_session$
    // $example on:programmatic_schema$
    // Create an RDD
    val uservisits = spark.sparkContext.textFile(uservisitsPath)
    val rankings = spark.sparkContext.textFile(rankingsPath)

    // The schema is encoded in a string
    val uservisitsSchemaString = "sourceIP destURL d1 d2 visitDate adRevenue userAgent countryCode languageCode searchWord duration"

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\||\\t"))
      .map(attributes => (attributes(2), attributes(6)))

    // The schema is encoded in a string
    val rankingsSchemaString = "pageURL pageRank avgDuration"


    // Convert records of the RDD (people) to Rows
    val rankingsRDD = rankings
      .map(_.split("\\||\\t"))
      .map(attributes => (attributes(1), attributes(2)))

    // Apply the schema to the RDD
    // val rankingsDF = spark.createDataFrame(rankingsRDD, rankingsSchema)

    // Creates a temporary view using the DataFrame
    // rankingsDF.createOrReplaceTempView("rankings")


    val result = uservisitsRDD.join(rankingsRDD)
    println(result.toDebugString)

    result.foreach(println)

  }
}
