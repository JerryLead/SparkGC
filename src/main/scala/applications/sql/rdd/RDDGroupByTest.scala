package applications.sql.rdd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by xulijie on 17-6-21.
  */
object RDDGroupByTest {
  def main(args: Array[String]): Unit = {

    // $example on:init_session$
    val spark = SparkSession
      .builder().master("local[2]")
      .appName("RDDGroupByTest")
      .getOrCreate()

    // $example off:init_session$
    // $example on:programmatic_schema$
    // Create an RDD

    val uservisits = spark.sparkContext.textFile("sampledata/sql/Rankings-UserVisits/UserVisits.txt")


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

    val result = uservisitsRDD.groupByKey(4)

    println(result.toDebugString)

    result.foreach(println)
  }

}
