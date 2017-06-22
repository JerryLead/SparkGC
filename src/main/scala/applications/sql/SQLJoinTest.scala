package applications.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by xulijie on 17-6-20.
  */
object SQLJoinTest {
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
    import spark.implicits._
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
      .map(attributes => Row(attributes(0), attributes(1), attributes(2)))

    // Apply the schema to the RDD
    val rankingsDF = spark.createDataFrame(rankingsRDD, rankingsSchema)

    // Creates a temporary view using the DataFrame
    rankingsDF.createOrReplaceTempView("rankings")



    // The schema is encoded in a string
    val uservisitsSchemaString = "sourceIP destURL visitDate adRevenue userAgent countryCode languageCode searchWord duration"

    // Generate the schema based on the string of schema
    val uservisitsFields = uservisitsSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val uservisitsSchema = StructType(uservisitsFields)

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\|"))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5),
        attributes(6), attributes(7), attributes(8)))

    // Apply the schema to the RDD
    val uservisitsDF = spark.createDataFrame(uservisitsRDD, uservisitsSchema)

    // Creates a temporary view using the DataFrame
    uservisitsDF.createOrReplaceTempView("uservisits")


    // SQL can be run over a temporary view created using DataFrames
    val result = spark.sql("SELECT * FROM rankings r JOIN uservisits uv ON r.pageURL = uv.destURL")

    result.write.save(args(2))

  }
}
