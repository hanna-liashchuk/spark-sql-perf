/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.sql.SparkSession

case class GenTPCDSTablesConfig(
    master: String = "local[*]",
    databaseName: String = "spark_benchmarks",
    dsdgenDir: String = null,
    scaleFactor: String = null,
    location: String = null,
    format: String = null,
    useDoubleForDecimal: Boolean = false,
    useStringForDate: Boolean = false,
    overwrite: Boolean = false,
    partitionTables: Boolean = true,
    clusterByPartitionColumns: Boolean = true,
    filterOutNullPartitionValues: Boolean = true,
    tableFilter: String = "",
    numPartitions: Int = 100)

/**
 * Gen TPCDS data.
 * To run this:
 * {{{
 *   build/sbt "test:runMain <this class> -d <dsdgenDir> -s <scaleFactor> -l <location> -f <format>"
 * }}}
 */
object GenTPCDSTables{
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[GenTPCDSTablesConfig]("Gen-TPC-DS-data") {
      opt[String]('m', "master")
        .action { (x, c) => c.copy(master = x) }
        .text("the Spark master to use, default to local[*]")
      opt[String]('d', "databaseName")
        .action { (x, c) => c.copy(databaseName = x) }
        .text("database name")
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = x))
        .text("root directory of location to create data in")
      opt[String]('f', "format")
        .action((x, c) => c.copy(format = x))
        .text("valid spark format, Parquet, ORC ...")
      opt[Boolean]('o', "overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .text("overwrite the data that is already there")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, GenTPCDSTablesConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  private def run(config: GenTPCDSTablesConfig) {
    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master(config.master)
      .getOrCreate()

    val tables = new TPCDSTables(spark.sqlContext,
      dsdgenDir = config.dsdgenDir,
      scaleFactor = config.scaleFactor,
      useDoubleForDecimal = config.useDoubleForDecimal,
      useStringForDate = config.useStringForDate)

    tables.createExternalTables(config.location, config.format, config.databaseName,
      overwrite = config.overwrite, discoverPartitions = false)
  }
}
