package applications.sql.rdd

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-20.
  *
  * SELECT URL, adRevenue, pageRank FROM Rankings As R, UserVisits As UV WHERE R.URL = UR.URL
  */
object RDDJoinTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: RDDJoinTest <table1_hdfs_file> <table2_hdfs_file> <output_file>")
      System.exit(1)
    }

    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .getOrCreate()

    // $example off:init_session$
    // $example on:programmatic_schema$
    // Create an RDD
    val uservisits = spark.sparkContext.textFile(args(0))
    val rankings = spark.sparkContext.textFile(args(1))

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


    val result = uservisitsRDD.join(rankingsRDD)

    result.saveAsTextFile(args(2))

  }
}
