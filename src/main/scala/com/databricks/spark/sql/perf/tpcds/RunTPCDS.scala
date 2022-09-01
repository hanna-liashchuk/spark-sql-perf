package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class RunConfig(
                      master: String = "local[*]",
                      databaseName: String = "spark_benchmarks",
                      resultLocation: String = "",
                      iterations: Int = 1,
                      queries: Seq[String] = Seq(),
                      timeout: Int = 24 * 60 * 60
                    )

object RunTPCDS {

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('m', "master")
        .action { (x, c) => c.copy(master = x) }
        .text("the Spark master to use, default to local[*]")
      opt[String]('d', "databaseName")
        .action { (x, c) => c.copy(databaseName = x) }
        .text("database name")
        .required()
      opt[String]('o', "resultLocation")
        .action((x, c) => c.copy(resultLocation = x))
        .text("Location to store the results of the benchmark")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[Seq[String]]('q', "queries")
        .action((x, c) => c.copy(queries = x))
        .text("queries to run, empty for a full run.")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf()
      .setMaster(config.master)
      .setAppName(getClass.getName)

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()
    val sqlContext = sparkSession.sqlContext

    val tpcds = new TPCDS(sqlContext = sqlContext)
    val queries = config.queries match {
        case Seq() => tpcds.tpcds2_4Queries
        case _ => tpcds.tpcds2_4Queries.filter(q => config.queries.contains(q.name))
    }
    sparkSession.sql(s"use ${config.databaseName}")
    val experiment = tpcds.runExperiment(
      queries,
      iterations = config.iterations,
      resultLocation = config.resultLocation,
      tags = Map("runtype" -> "benchmark", "database" -> config.databaseName))

    experiment.waitForFinish(config.timeout)

  }
}
