# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets   # this will allow to refer another notebook within this notebook

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")  #dispaly the list of files in the folder
display(files)

# COMMAND ----------

#df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
#display(df_raw)

df_raw = spark.read.format("json").load(f"{dataset_bookstore}/kafka-raw") # reading data from json file into data frame
display(df_raw)


# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():  #defining UDF function
  
    #schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
    schema = "key binary, value binary, topic string, partition long, offset long, timestamp long"
    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                      .option("mergeSchema", True)
                      .partitionBy("topic", "year_month") #partitioning while writing to stream
                      .trigger(availableNow=True) # to show how often streaming query shd be executed
                      .table("bronze"))
    
    query.awaitTermination() # wait till the manual terminate is received or any condition is met to get terminated
    

    # trigger - available = true will tell you to process all the available data and stops once the data is done.effective for file based streaming 4 modes. default - processes micro batch/fixed time processing/batch processing/continousmode - 1 sec check point



# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze") #reading the bronze table
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic) FROM bronze
# MAGIC
# MAGIC SELECT * from bronze
# MAGIC
# MAGIC

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

proc = process_bronze()
display(proc)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------


