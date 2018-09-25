package com.apple.jobs.codingchallenge

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeZone}
import org.rogach.scallop.ScallopConf
import org.yaml.snakeyaml.Yaml
import net.jcazevedo.moultingyaml._
import org.yaml.snakeyaml.constructor.Constructor

import collection.JavaConversions._
import scala.io.Source

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val config = opt[String](required = false)
  val date = opt[String](required = false)
  verify()
}

object ConfigYamlProtocol extends DefaultYamlProtocol {
  implicit val outputDataset = yamlFormat1(OutputPaths)
  implicit val configOrderActvityFormat = yamlFormat2(
    ConfigActvity)
}

object MainJob extends Pipeline {
  def main(args: Array[String]): Unit = {

    val cli = new Conf(args)
    val config = fileReader(cli.config.getOrElse(""))
    val currentTime =
      new DateTime(cli.date.getOrElse(DateTime.now(DateTimeZone.UTC).toString))
    import ConfigYamlProtocol._
    val appleConfig =
      config.parseYaml.convertTo[ConfigActvity]

    val conf: SparkConf = new SparkConf()

    for ((k, v) <- appleConfig.sparkConf
           .split(",|=")
           .grouped(2)
           .map { case Array(k, v) => k -> v }
           .toMap) yield {
      conf.set(k, v)
    }
    val spark = SparkSession
      .builder()
      .appName("apple-coding-challenge")
      .config(conf)
      .getOrCreate()

    executeJob(spark, appleConfig, currentTime)
  }

  def fileReader(filename: String): String = {
    Source.fromFile(filename).mkString
  }
}

case class OutputPaths(
    outputPath: String
)


case class ConfigActvity(
    paths: OutputPaths,
    sparkConf: String
)
