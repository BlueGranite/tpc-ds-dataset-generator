# tpc-ds-dataset-generator

This project contains notebooks that are used to generate <a href="http://www.tpc.org/tpcds/default.asp" target="_blank">TPC-DS</a>  datasets using the [Databricks performance testing framework](https://github.com/databricks/spark-sql-perf) for <a href="https://spark.apache.org/sql/" target="_blank">Spark SQL</a> (spark-sql-perf). The framework can be used for running various performance tests, but the motivation for this project was the generation of TPC-DS data.

TPC-DS is an industry benchmark, but the dataset is also useful for POCs, demos, and performance testing. An advantage to the TPC-DS generator is that it supports datasets ranging in size from 1GB to 100TB. The data is modeled as a star schema with some snowflake tables.

![TPC-DS scales](https://github.com/BlueGranite/tpc-ds-dataset-generator/blob/master/img/tpcds_scales.png)

The stock TPC-DS generator only supports delimited files. spark-sql-perf adds the following features:

- Additional file formats such as parquet
- File partitioning
- Database creation with optional statistics collection

Follow the instructions in the TPC-DS-Configure notebook to prepare the cluster. Then you can use the TPC-DS-GenerateData notebook to generate the datasets.