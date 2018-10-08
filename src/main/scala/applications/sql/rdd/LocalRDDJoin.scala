package applications.sql.rdd

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-20.
  *
  * SELECT * FROM Rankings As R, UserVisits As UV WHERE R.pageURL = UR.destURL
  */
object LocalRDDJoin {
  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()


    val uservisitsPath = "/Users/xulijie/Documents/data/SQLdata/UserVisits-100.txt"
    val rankingsPath = "/Users/xulijie/Documents/data/SQLdata/Rankings-100.txt"

    // $example off:init_session$
    // $example on:programmatic_schema$
    // Create an RDD
    val rankings = spark.sparkContext.textFile(rankingsPath)
    val uservisits = spark.sparkContext.textFile(uservisitsPath)

    // The schema is encoded in a string
    val rankingsSchemaString = "pageRank pageURL avgDuration"


    // Convert records of the RDD (people) to Rows
    val rankingsRDD = rankings
      .map(_.split("\\|"))
      .map(attributes => (attributes(1), Row(attributes(0), attributes(1), attributes(2))))

    // Apply the schema to the RDD
    // val rankingsDF = spark.createDataFrame(rankingsRDD, rankingsSchema)

    // Creates a temporary view using the DataFrame
    // rankingsDF.createOrReplaceTempView("rankings")



    // The schema is encoded in a string
    val uservisitsSchemaString = "sourceIP destURL visitDate adRevenue userAgent countryCode languageCode searchWord duration"

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\|"))
      .map(attributes => (attributes(1), Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5),
        attributes(6), attributes(7), attributes(8))))

    val result = rankingsRDD.join(uservisitsRDD)

    result.foreach(println)

  }
}
