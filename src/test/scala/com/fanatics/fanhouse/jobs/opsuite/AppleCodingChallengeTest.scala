package com.apple.jobs.codingchallenge

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SparkSessionProvider}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{lit, current_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import java.sql.{Date, Timestamp}
import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}

class AppleCodingTest
    extends FunSuite
    with DatasetSuiteBase
    with Matchers
    with BeforeAndAfterEach
    with Pipeline{
  var currentTime: Long = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext.sparkContext.setLogLevel("OFF")
    currentTime = System.currentTimeMillis
  }

  override def afterAll(): Unit = {
    sqlContext.clearCache()
    super.afterAll()
  }

  test("Test Return for Order Actvty") {
    val session = SparkSessionProvider.sparkSession
    val transLineItem = generateReturnOrderActvty

    val ordActvty = OrdActvtyRetJob.createOrdActAndRetTypeId(transLineItem)
    // 40 - Item Returned
    val orderedItems = ordActvty.filter(x => x.actvty_type_id === 40).head
    assert(orderedItems.actvty_lc_amt === 12.34)

    // 43 - Item Discount Return
    val actvtyItemDiscountAppliedObj =
      ordActvty.filter(x => x.actvty_type_id === 43)

    assert(actvtyItemDiscountAppliedObj === Nil)
  }


  def generateItemLocationDetail(): DataFrame = {
    val itemLocationDetailList = List(
      ItemLocationDetail(200, 55, 200),
      ItemLocationDetail(300, 458, 300),
      ItemLocationDetail(400, 732, 400)
    )
    sqlContext.createDataFrame(sc.parallelize(itemLocationDetailList))
  }
 
}