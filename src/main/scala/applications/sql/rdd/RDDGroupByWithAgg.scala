package applications.sql.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by xulijie on 18-9-27.
  *
  * SELECT sourceIP, visitDate, SUM(adRevenue) FROM uservisits GROUP BY sourceIP, visitDate;
  */
object RDDGroupByWithAgg {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: RDDGroupByWithAgg <table_hdfs_file> <output_file>")
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

    // Generate the schema based on the string of schema
    val uservisitsSchema = StructType(
      List(
        StructField("sourceIP", StringType, true),
        StructField("destURL", StringType, true),
        StructField("visitDate", StringType, true),
        StructField("adRevenue", DoubleType, true),
        StructField("userAgent", StringType, true),
        StructField("countryCode", StringType, true),
        StructField("languageCode", StringType, true),
        StructField("searchWord", StringType, true),
        StructField("duration", IntegerType, true)
      )
    )

    // Convert records of the RDD (people) to Rows
    val uservisitsRDD = uservisits
      .map(_.split("\\||\\t"))
      .map(attributes => ((attributes(1), attributes(3)), attributes(4).toDouble))

    val results = uservisitsRDD.aggregateByKey(0.0)((a, b) => a + b, (a, b) => a + b)

    results.saveAsTextFile(args(1))
  }

}
