from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic = 'order-topic'
kafka_bootstrap_servers = 'localhost:9092'
customers_file_path = '/data/customers.csv'

mysql_host_name = 'localhost'
mysql_port_no = '3306'
mysql_db_name = 'order_db'
mysql_driver_class = 'com.mysql.cj.jdbc.Driver'
mysql_table_name = 'total_sales'
mysql_username = 'root'
mysql_password = 'mydellpx'
mysql_jdbc_url = 'jdbc:mysql://' + mysql_host_name + ':' + mysql_port_no + '/' + mysql_db_name

cassandra_host_name = 'localhost'
cassandra_port = '9042'
cassandra_keyspace = 'orders_ks'
cassandra_table = 'orders_tbl'

customers_file_path = "/user/aravind/data/customers.csv"

def write_to_cassandra(df, epoch_id):
    # Cassandra connection parameters

    filtered_df = df.filter(col("order_id").isNotNull())
    final_df = filtered_df.withColumn("timestamp", current_timestamp())

    # Write data to Cassandra table
    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", cassandra_keyspace) \
        .option("table", cassandra_table) \
        .save()

def write_to_sql(df, epoch_id):
    mysql_properties = {
                        "user": mysql_username,
                        "password": mysql_password,
                        "driver": mysql_driver_class
    }

    # Write the result to MySQL
    df.write.jdbc(mysql_jdbc_url, mysql_table_name, mode="append", properties=mysql_properties)

if __name__ == "__main__":
    print("Entering Data Streaming session")
    # Spark session
    spark = SparkSession.builder.appName("KafkaStructuredStreaming") \
    .config("spark.cassandra.connection.host", cassandra_host_name) \
    .config("spark.cassandra.connection.port", cassandra_port) \
    .getOrCreate()

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("created_at", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("subtotal", StringType(), True),
        StructField("tax", StringType(), True),
        StructField("total", StringType(), True),
        StructField("customer_id", IntegerType(), True)  
    ])


    # Read data from Kafka in JSON format
    raw_data = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()
    print("Printing data schema: ")
    print(raw_data.printSchema())
    
    # Parse the JSON data
    parsed_data = raw_data.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Process the data and write it in cassandra
    query_cassandra = parsed_data.writeStream \
                    .trigger(processingTime='10 seconds') \
                    .outputMode("append") \
                    .foreachBatch(write_to_cassandra) \
                    .start()

    customer_df = spark.read.csv(customers_file_path, header=True, inferSchema=True)
    customer_df.printSchema()

    # Join streaming data with static customer data
    joined_df = parsed_data.join(customer_df, parsed_data.customer_id == customer_df.ID, "inner")

    # Perform aggregations
    aggr_df = joined_df.groupBy("Source", "State").agg(sum("total").alias("total_sum_amount"))

    print("Printing schema of Aggregate Data:")
    aggr_df.printSchema()

    final_df = aggr_df.withColumn("processed_at", current_timestamp())

    query_sql = final_df.writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("complete") \
        .foreachBatch(write_to_sql) \
        .start()

    # Await termination of the streaming query
    query_cassandra.awaitTermination()
    query_sql.awaitTermination()