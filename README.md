# tpc-ds-dataset-generator
Generate big TPC-DS datasets with Databricks

This project contains notebooks that are used to generate [TPC-DS](http://www.tpc.org/tpcds/default.asp) datasets using the [Databricks performance testing framework](https://github.com/databricks/spark-sql-perf) for [Spark SQL](https://spark.apache.org/sql/) (spark-sql-perf). The framework can be used for running various performance tests, but the motivation for this project was the generation of TPC-DS data.

TPC-DS is an industry benchmark, but the dataset is also useful for POCs, demos, and performance testing. An advantage to the TPC-DS generator is that it supports datasets ranging in size from 1GB to 100TB. The data is modeled as a star schema with some snowflake tables.

![TPC-DS scales](https://https://github.com/BlueGranite/tpc-ds-dataset-generator/img/tpcds_scales.png)

The stock TPC-DS generator only supports delimited files. spark-sql-perf adds the following features:

- Additional file formats such as parquet
- File partitioning
- Database creation with optional statistics collection

## Overview
Generating TPC-DS datasets with spark-sql-perf involves the following steps.

1. Build the spark-sql-perf library jar using [sbt](https://www.scala-sbt.org/1.x/docs/index.html) and add the jar to your cluster.
2. Run the TPC-DS-Configure notebook, which will:
   1. Mount an Azure Storage account.
   2. Install the Databricks [TPC-DS benchmark kit](https://github.com/databricks/tpcds-kit).
   3. Restart the cluster.
3. Run the TPC-DS-Generate notebook to generate the data and setup the database.