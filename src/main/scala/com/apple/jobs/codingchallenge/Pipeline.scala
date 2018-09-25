package com.apple.jobs.codingchallenge

import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  SQLContext,
  SaveMode,
  SparkSession,
  functions => SSqlFunc
}
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import collection.JavaConversions._

trait Pipeline extends Serializable {

  /** Executes the Apple Coding Challenge Pipeline,
    *
    * *
    *
    * @param spark                - Spark Session Object
    * @param orderActvtyRetConfig - config utility
    * @return - .
    */
  def executeJob(spark: SparkSession,
                 appleConfig: ConfigActvity,
                 currDateTime: DateTime) = {

    import spark.implicits._
    val dataFrameSubscriptionFormat = "json"

    val readDataFrame: (SQLContext, String, List[String]) => DataFrame =
      (sqlContext, format, paths) => {

        val df = sqlContext.read.format(format).load(paths: _*)
        //spark.read.option("prefersDecimal", "true")
        //          .parquet("s3://fanatics.prod.internal.confidential/nkudre/manhattan/80ec6380b93911e8ad3ffd7261809e5a/20180919/pix_TranDF/*")
        df
      }

    //val getCurrentView = (readDataFrame _).curried(spark)(dataFrameSubscriptionFormat)

  }
}
