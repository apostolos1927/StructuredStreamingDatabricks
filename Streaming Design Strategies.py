# Databricks notebook source
# MAGIC %md
# MAGIC ## Structured Streaming and Delta Lake make incremental ETL easy

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE bronze_layer 
# MAGIC (id INT, first_name STRING, age DOUBLE); 
# MAGIC
# MAGIC INSERT INTO bronze_layer
# MAGIC VALUES (1, "Mike", 45),
# MAGIC        (2, "Omar", 23),
# MAGIC        (3, "Nick", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_layer

# COMMAND ----------

# MAGIC %md
# MAGIC Using structured streaming we add a field to capture the timestamp and write the data into the silver table as a single batch.
# MAGIC
# MAGIC While this code uses Structured Streaming, it's appropriate to think of this as a triggered batch processing incremental changes.
# MAGIC
# MAGIC  **`trigger(availableNow=True)`** to slow down the processing of the data combined with **`query.awaitTermination()`** to wait until the one batch is processed.  **trigger-available-now** is very similar to **trigger-once** but can run multiple micro-batches 

# COMMAND ----------

from pyspark.sql import functions as F

def run_silver():
    query = (spark.readStream
                  .table("bronze_layer")
                  .withColumn("processed_time", F.current_timestamp())
                  .writeStream.option("checkpointLocation", f"/FileStore/silver")
                  .trigger(availableNow=True)
                  .table("silver_layer"))
    
    query.awaitTermination()


# COMMAND ----------

run_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_layer
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake is ideally suited for easily tracking and propagating inserted data through a series of tables. This pattern has a number of names, including "medallion", "multi-hop", "Delta", and "bronze/silver/gold" architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Multiple Tables
# MAGIC  **`foreachBatch`** method provides the option to execute custom data writing logic on each microbatch of streaming data.
# MAGIC
# MAGIC The Databricks guarantees that streaming Delta Lake writes will be idempotent, even when writing to multiple tables, if you set the "txnVersion" and "txnAppId" options.

# COMMAND ----------

def write_two_sinks(microBatchDF, batchId):
    appId = "write_two_sinks"
    print('batchId == ',batchId)
    microBatchDF.select("id", "first_name", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_first_name")
    
    microBatchDF.select("id", "age", F.current_timestamp().alias("processed_time")).write.option("txnVersion", batchId).option("txnAppId", appId).mode("append").saveAsTable("silver_age")


def two_streams():
    query = (spark.readStream.table("bronze_layer")
                 .writeStream
                 .foreachBatch(write_two_sinks)
                 .option("checkpointLocation", f"FileStore/two_streams")
                 .trigger(once=True)
                 .start())
    
    query.awaitTermination()
    

# COMMAND ----------

two_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC The cells below demonstrate the logic was applied properly to split the initial data into two tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_first_name
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_age
# MAGIC ORDER BY processed_time DESC, id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC We capture the current timestamps at the time each write executes, so while both writes happen within the same streaming microbatch process, they are fully independent transactions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Aggregates
# MAGIC
# MAGIC Incremental aggregation can be useful for a number of purposes, including dashboarding and enriching reports with current summary data.
# MAGIC
# MAGIC The logic below defines a handful of aggregations against the **`silver`** table.

# COMMAND ----------

def define_aggregates():
    query = (spark.readStream
                  .table("silver_layer")
                  .groupBy("id")
                  .agg(F.sum("age").alias("total_age"), 
                       F.mean("age").alias("avg_age"),
                       F.count("age").alias("record_count"))
                  .writeStream
                  .option("checkpointLocation", f"FileStore/aggregates")
                  .outputMode("complete")
                  .trigger(once=True)
                  .table("silver_aggregates"))
    
    query.awaitTermination()
    

# COMMAND ----------

define_aggregates()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_aggregates
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC One thing to note is that the logic being executed is currently overwriting the resulting table with each write. We can use MERGE with foreachBatch to update existing records. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Change Data Capture Data
# MAGIC Here the **`bronze_cdc`** table will represent the raw CDC information

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze_cdc 
# MAGIC (user_id INT, first_name STRING, update_type STRING, processed_timestamp TIMESTAMP);
# MAGIC
# MAGIC INSERT INTO bronze_cdc
# MAGIC VALUES  (1, "James", "insert", current_timestamp()),
# MAGIC         (2, "John", "update", current_timestamp()),
# MAGIC         (3, "Kimberly", "update", current_timestamp()),
# MAGIC         (4, "Irene", "update", current_timestamp()),
# MAGIC         (5, null, "delete", current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver_cdc (user_id INT, first_name STRING, updated_timestamp TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC The **`MERGE`** statement can easily be written with SQL to apply CDC changes appropriately, given the type of update received.

# COMMAND ----------

def upsert_cdc(microBatchDF, batchID):
    microBatchDF.createTempView("bronze_cdc_batch")
    
    query = """
        MERGE INTO silver_cdc s
        USING bronze_cdc_batch b
        ON b.user_id = s.user_id
        WHEN MATCHED AND b.update_type = "update"
          THEN UPDATE SET user_id=b.user_id, first_name=b.first_name, updated_timestamp=b.processed_timestamp
        WHEN MATCHED AND b.update_type = "delete"
          THEN DELETE
        WHEN NOT MATCHED AND b.update_type = "update" OR b.update_type = "insert"
          THEN INSERT (user_id, first_name, updated_timestamp)
          VALUES (b.user_id, b.first_name, b.processed_timestamp)
    """
    # access the local spark session in foreachbatch
    microBatchDF._jdf.sparkSession().sql(query)
    
def cdc_merge():
    query = (spark.readStream
                  .table("bronze_cdc")
                  .writeStream
                  .foreachBatch(upsert_cdc)
                  .option("checkpointLocation", f"FileStore/silver_cdc")
                  .trigger(availableNow=True)
                  .start())
    
    query.awaitTermination()
    

# COMMAND ----------

# MAGIC %md
# MAGIC As always, we incrementally process newly arriving records.

# COMMAND ----------

cdc_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_cdc
# MAGIC ORDER BY updated_timestamp DESC, user_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Inserting new records will allow us to then apply these changes to our silver data.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_cdc
# MAGIC VALUES  (1, "Lala", "update", current_timestamp()),
# MAGIC         (2, "T4est", "update", current_timestamp()),
# MAGIC         (3, "Apo", "update", current_timestamp()),
# MAGIC         (4, null, "delete", current_timestamp()),
# MAGIC         (6, "Test123", "insert", current_timestamp())

# COMMAND ----------

cdc_merge()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_cdc 
# MAGIC ORDER BY updated_timestamp DESC, user_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Two Incremental Tables
# MAGIC
# MAGIC Note that there are many intricacies around watermarking and windows when dealing with incremental joins, and that not all join types are supported.

# COMMAND ----------

def incremental_stream_join():
    first_nameDF = spark.readStream.table("silver_first_name")
    ageDF = spark.readStream.table("silver_age")
    
    return (first_nameDF.join(ageDF, first_nameDF.id == ageDF.id, "inner")
                  .select(first_nameDF.id, 
                          first_nameDF.first_name, 
                          ageDF.age, 
                          F.current_timestamp().alias("joined_timestamp"))
                  .writeStream
                  .option("checkpointLocation", f"FileStore/joined_streams")
                  .table("incremental_joined_streams")
           )

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the logic defined above does not set a **`trigger`** option.
# MAGIC
# MAGIC This means that the stream will run in continuous execution mode, triggering every 500ms by default.

# COMMAND ----------

incremental_stream_join()

# COMMAND ----------

# MAGIC %md
# MAGIC Because the stream never stops we can't block until the trigger-available-now stream has terminated with **`awaitTermination()`**.

# COMMAND ----------

# MAGIC %md
# MAGIC Running **`display()`** on a streaming table is a way to monitor table updates in near-real-time while in interactive development. 

# COMMAND ----------

display(spark.readStream.table("incremental_joined_streams"))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> Anytime a streaming read is displayed to a notebook, a streaming job will begin.
# MAGIC
# MAGIC Here a second stream is started.  One is processing the data as part of our original pipline, and now a second streaming job is running to update the **`display()`** function with the latest results.

# COMMAND ----------

# MAGIC %md
# MAGIC Here we'll add new values to the **`bronze`** table.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_layer
# MAGIC VALUES (10, "Pedro", 10.5),
# MAGIC        (11, "Amelia", 11.5),
# MAGIC        (12, "Diya", 12.3),
# MAGIC        (13, "Li", 13.4),
# MAGIC        (14, "Daiyu", 14.2),
# MAGIC        (15, "Jacques", 15.9)

# COMMAND ----------

two_streams()

# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping {stream.name}")
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Incremental and Static Data

# COMMAND ----------

statusDF = spark.read.table("silver_cdc")
bronzeDF = spark.readStream.table("bronze_layer")

query = (bronzeDF.alias("bronze")
                 .join(statusDF.alias("status"), bronzeDF.id==statusDF.user_id, "inner")
                 .select("bronze.*")
                 .writeStream
                 .option("checkpointLocation", f"FileStore/join_static_status")
                 .queryName("joined_status_query")
                 .table("joined_static_status")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joined_static_status
# MAGIC ORDER BY id DESC
