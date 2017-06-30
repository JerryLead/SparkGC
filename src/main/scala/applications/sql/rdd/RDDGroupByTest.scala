package applications.sql.rdd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by xulijie on 17-6-21.
  *
  * SELECT * FROM UserVisits GROUP BY SUBSTR(sourceIP, 1, 7);
  */
object RDDGroupByTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: RDDGroupByTest <table_hdfs_file> <output_file>")
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


    // The schema is encoded in a string
    val uservisitsSchemaString = "sourceIP destURL visitDate adRevenue userAgent countryCode languageCode searchWord duration"

    // Generate the schema based on the string of schema
    val uservisitsFields = uservisitsSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val uservisitsSchema = StructType(uservisitsFields)

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\|"))
      .map(attributes => (attributes(0).substring(0, 7), Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5),
        attributes(6), attributes(7), attributes(8))))

    // Apply the schema to the RDD
    // val uservisitsDF = spark.createDataFrame(uservisitsRDD, uservisitsSchema)

    // Creates a temporary view using the DataFrame
    // uservisitsDF.createOrReplaceTempView("uservisits")

    // SQL can be run over a temporary view created using DataFrames
    // val results = spark.sql("SELECT name FROM people")

    val result = uservisitsRDD.groupByKey()

    result.saveAsTextFile(args(1))
  }

}
