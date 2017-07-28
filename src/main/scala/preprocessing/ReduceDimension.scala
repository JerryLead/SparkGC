package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Shen on 2017/4/20.
  */
object ReduceDimension {
  def reduce(
              sc: SparkContext,
              path: String,
              reduction: Double, // 0.25
              path_new: String,
              numPartitions: Int
            ):
  Unit ={
    val data = sc.textFile(path)

    val numFeatures_ori = computeNumFeatures(data)
    val reduce_data = data.map(s =>
      s.split(' ').filter(isFilter(reduction, _))
        .map(x =>
          if (x.split(":").length > 1)
          {(x.split(":")(0).toInt-(x.split(":")(0).toInt/(1/reduction).toInt) * ((1/reduction).toInt-1)).toString()+":"+x.split(":")(1)}
          else x
        )
    )
      .map(row => row.mkString(" "))

    val numFeatures_new = computeNumFeatures(reduce_data)

    reduce_data.coalesce(numPartitions).saveAsTextFile(path_new)

    println(s"n_features of original data = $numFeatures_ori")
    println(s"n_features of new data = $numFeatures_new")
  }

  def reduce2(
               sc: SparkContext,
               path: String,
               reduction: Double, // 0.75
               path_new: String,
               numPartitions: Int
             ):
  Unit ={
    val data = sc.textFile(path)
    val numFeatures_ori = computeNumFeatures(data)
    val reduce_data = data.map(s =>
      s.split(' ').filter(isFilter(reduction, _))
        .map(x =>
          if (x.split(":").length > 1)
          {(x.split(":")(0).toInt-(x.split(":")(0).toInt/(1/(1-reduction)).toInt)).toString()+":"+x.split(":")(1)}
          else x
        )
    )
      .map(row => row.mkString(" "))

    val numFeatures_new = computeNumFeatures(reduce_data)

    reduce_data.coalesce(numPartitions).saveAsTextFile(path_new)

    println(s"n_features of original data = $numFeatures_ori")
    println(s"n_features of new data = $numFeatures_new")
  }

  def isFilter( reduction: Double, x: String): Boolean={
    x.split(':').length < 2 || x.split(':')(0).toInt % (1/(1-reduction)) != 0
  }


  def computeNumFeatures(data: RDD[String]): Int = {
    data.map { s =>
      val n = s.split(' ').length
      s.split(' ')(n-1).split(":")(0).toInt
    }.reduce(math.max)
  }

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
      .setAppName("ReduceDimension")
    //      .setMaster("local")
    val sc = new SparkContext(conf)
    //    val path = "data/redcueOri"
    val path = args(0)
    //    val reduction = 0.5
    // reduction must be [0.1, 0.125, 0.2, 0.25, 0.5, 0.75, 0.875, 0.9]
    val reduction = args(1).toDouble
    //    val path_new = "data/redcue0.5"
    val path_new = args(2)
    val numPartitions = args(3).toInt
    //    val numPartitions = 1
    if (reduction <= 0.5){
      reduce(sc, path, reduction, path_new, numPartitions)
    }
    else{
      reduce2(sc, path, reduction, path_new, numPartitions)
    }

  }

}