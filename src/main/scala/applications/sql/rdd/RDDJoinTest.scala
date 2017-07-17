package applications.sql.rdd

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by xulijie on 17-6-20.
  *
  * SELECT * FROM Rankings As R, UserVisits As UV WHERE R.pageURL = UR.destURL
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
    val rankings = spark.sparkContext.textFile(args(0))
    val uservisits = spark.sparkContext.textFile(args(1))

    // The schema is encoded in a string
    val rankingsSchemaString = "pageRank pageURL avgDuration"

    // Generate the schema based on the string of schema
    val rankingsFields = rankingsSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val rankingsSchema = StructType(rankingsFields)

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

    // Generate the schema based on the string of schema
    val uservisitsFields = uservisitsSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val uservisitsSchema = StructType(uservisitsFields)

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\|"))
      .map(attributes => (attributes(1), Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5),
        attributes(6), attributes(7), attributes(8))))

    val result = rankingsRDD.join(uservisitsRDD)

    result.saveAsTextFile(args(2))

  }
}
