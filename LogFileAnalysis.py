from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LogFileAnalysis").getOrCreate()

df = spark.read.format("json").option("multiline", True).load("dbfs:/FileStore/shared_uploads/amarnathreddy1808@gmail.com/sample_logs.json")

df.show()



from pyspark.sql.functions import col, count, desc
from datetime import datetime, timedelta

#for last 7 days 

today = datetime.now()
start_date = today - timedelta(days=7)

#Identify the top 3 servers with the highest number of ERROR logs over the past week

top_servers = (
    df.filter(col("timestamp").between(start_date, today))
           .filter(col("log_level") == "ERROR")
           .groupBy("server_id")  
           .agg(count("*").alias("error_count"))
           .orderBy(desc("error_count"))
           .limit(3)
)
 
top_servers.show()

#Calculate the average number of logs generated per day by each server over the past week. 

from pyspark.sql.functions import  lit

average_logs_per_day = (
            df.filter(col("timestamp").between(start_date, today))
                .groupBy("server_id") 
                .agg(count("*").alias("total_logs"))
                .withColumn("average_logs_per_day", col("total_logs") / lit(7))
                .orderBy(desc("average_logs_per_day"))
)

average_logs_per_day.show()

#Provide a summary report of the most common log messages for each severity level. 

log_summary = (
    df.groupBy("log_level", "message")  
           .agg(count("*").alias("message_count"))
           .orderBy("log_level", desc("message_count"))
)


log_summary.show()