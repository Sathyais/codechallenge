package com.apple.integration.service

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.apple.integration.utils._
import org.apache.spark.sql.hive.HiveContext 
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.broadcast 
import com.google.gson.Gson  

object JsonParsingService extends LogHelper {

  def main(args: Array[String]) {
    val jsonhdfsPath = args(0)
    displayAndPrintJsonDataFrame(jsonhdfsPath)
  }

  def displayAndPrintJsonDataFrame(hdfsPath: String) {

    lazy val sc: SparkContext = {
      val conf = new SparkConf().setMaster("local")
        .setAppName("sathya_apple")
        .set("spark.driver.allowMultipleContexts", "false")
      new SparkContext(conf)
    }
    lazy val hiveContext = new HiveContext(sc)

    // Create the Data-frame from the json file on HDFS
    /*
     * 
       dfJson.show
            +------+--------+--------------------+
            |deptId|deptName|                 emp|
            +------+--------+--------------------+
            |    10|      IT| [[4,Mike], [5,Raj]]|
            |    20|      HR|[[5,Matt], [7,Paul]]|
            +------+--------+--------------------+
     */
    val dfJson = hiveContext.read.json(hdfsPath)
    // Register the Data-frame
    dfJson.registerTempTable("dfJson")
    // LATERAL VIEW with explode would have the output as below
    /*
        +------+--------+-----+-------+
        |deptId|deptName|empID|empName|
        +------+--------+-----+-------+
        |    10|      IT|    4|   Mike|
        |    10|      IT|    5|    Raj|
        |    20|      HR|    5|   Matt|
        |    20|      HR|    7|   Paul|
        +------+--------+-----+-------+
      */
    val dfLat = hiveContext.sql("select deptId, deptName, empData.empID,empData.empName from dfJson LATERAL VIEW explode(emp) empTable as empData")
    dfLat.registerTempTable("dfLat")

    /*
     *   collect_set would would combine the rows with comma seperated values
     * +------+--------+------+------------+                                           
      |deptId|deptName|empIds|    empNames|
      +------+--------+------+------------+
      |    20|      HR|[6, 7]|[Mike, Paul]|
      |    10|      IT|[4, 5]| [Matt, Raj]|
      +------+--------+------+------------+
     */

    val grpedDf = hiveContext.sql("select deptId,deptName,collect_set(empID) as empIds,collect_set(empName) as empNames from dfLat group by deptId,deptName")

    /*
 *  Using selectExpr , seperate the comma seperated value
        *  +------+--------+-------+-------+---------+---------+                           
        |deptId|deptName|emp_id1|emp_id2|emp_name1|emp_name2|
        +------+--------+-------+-------+---------+---------+
        |    20|      HR|      6|      7|     Mike|     Paul|
        |    10|      IT|      4|      5|     Matt|      Raj|
      +------+--------+-------+-------+---------+---------+
 */

    val dfResult = grpedDf.selectExpr("deptId", "deptName", "empIds[0] as emp_id1", "empIds[1] as emp_id2", "empNames[0] as emp_name1", "empNames[1] as emp_name2")

    // Alternate method is using UDF function. 

    dfResult.show

  }

}