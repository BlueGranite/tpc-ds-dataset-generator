# Databricks notebook source
# DBTITLE 1,Generate TPC-DS data
# MAGIC %md
# MAGIC Generating data at larger scales can take hours to run, and you may want to run the notebook as a job.
# MAGIC 
# MAGIC The cell below generates the data. Read the code carefully, as it contains many parameters to control the process. See the <a href="https://github.com/databricks/spark-sql-perf" target="_blank">Databricks spark-sql-perf repository README</a> for more information.

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC 
# MAGIC // Set:
# MAGIC val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC 
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC 
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC 
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC 
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = "", // "" means generate all tables
# MAGIC     numPartitions = 4) // how many dsdgen partitions to run - number of input tasks.
# MAGIC 
# MAGIC // Create the specified database
# MAGIC sql(s"create database $databaseName")
# MAGIC 
# MAGIC // Create the specified database
# MAGIC sql(s"create database $databaseName")
# MAGIC 
# MAGIC // Create metastore tables in a specified database for your data.
# MAGIC // Once tables are created, the current database will be switched to the specified database.
# MAGIC tables.createExternalTables(rootDir, fileFormat, databaseName, overwrite = true, discoverPartitions = true)
# MAGIC 
# MAGIC // Or, if you want to create temporary tables
# MAGIC // tables.createTemporaryTables(location, fileFormat)
# MAGIC 
# MAGIC // For Cost-based optimizer (CBO) only, gather statistics on all columns:
# MAGIC tables.analyzeTables(databaseName, analyzeColumns = true)

# COMMAND ----------

# DBTITLE 1,View TPC-DS data
# examine data
df = spark.read.parquet("/mnt/datalake/raw/tpc-ds/source_files_001TB_parquet/customer")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Sample Results
# MAGIC Below are a few sample results from generating data at the 1 and 1000 scale.
# MAGIC 
# MAGIC | File Format | Generate Column Stats | Number of dsdgen Tasks | Partition Tables | TPC-DS Scale | Databricks Cluster Config               | Duration | Storage Size |
# MAGIC | ----------- | --------------------- | ---------------------- | ---------------- | ------------ | --------------------------------------- | -------- | ------------ |
# MAGIC | csv | no | 4 | no | 1 | 1 Standard_DS3_v2 worker, 4 total cores | 4.79 min | 1.2 GB |
# MAGIC | parquet     | yes                   | 4                      | no               | 1            | 1 Standard_DS3_v2 worker, 4 total cores  | 5.88 min | 347 MB |
# MAGIC | json | no | 4 | no | 1 | 1 Standard_DS3_v2 worker, 4 total cores | 7.35 min | 5.15 GB |
# MAGIC | parquet | yes | 1000 | yes | 1000 | 4 Standard_DS3_v2 worker, 16 total cores | 4 hours | 333 GB |
