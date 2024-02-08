from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster


# Tạo SparkSession
# spark = SparkSession.builder \
# .appName("SparkKafkaCassandraIntegration") \
# .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
# .getOrCreate()

def create_spark_session(app_name):
    return   SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .getOrCreate()

def read_from_kafka(spark, bootstrap_servers, topic):
    return  spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

def parse_json_column(df, schema):
    return df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")

def create_mongo_sink(uri, database, collection):
    return lambda df, epoch_id: df.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .mode("append") \
        .option("uri",f"mongodb://{uri}/{database}.{collection}") \
        .save()

def parse_json_column(df, schema):
    return df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")


def main():
    spark_session = create_spark_session("Job_Streaming")

    # Đặt cấu hình Kafka
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic_job = "jobs"
    kafka_topic_company = "companies"

    # Đọc dữ liệu từ Kafka
    kafka_df_job = read_from_kafka(spark_session, kafka_bootstrap_servers, kafka_topic_job)
    kafka_df_company = read_from_kafka(spark_session, kafka_bootstrap_servers, kafka_topic_company)

    schema_job = StructType([
        StructField("job_name", StringType(), False),
        StructField("job_url", StringType(), False),
        StructField("type", StringType(), False),
        StructField("location", StringType(), False),
        StructField("company", StringType(), False),
        StructField("tag", StringType(), False),
        StructField("post_time", StringType(), False),
    ])

    schema_company = StructType([
        StructField("company_name", StringType(), False),
        StructField("company_url", StringType(), False),
        StructField("type", StringType(), False),
        StructField("size", StringType(), False),
        StructField("working_day", StringType(), False),
        StructField("country", StringType(), False),
        StructField("location", StringType(), False),
        StructField("job_quantity", StringType(), False),
    ])

    database_url = "localhost:27017"
    database = "Gr2"
    collection_job = "job"
    collection_company = "company"

    
    mongo_sink_job = create_mongo_sink(database_url, database, collection_job)
    mongo_sink_company = create_mongo_sink(database_url, database, collection_company)

    selection_df_job = parse_json_column(kafka_df_job, schema_job)

    selection_df_company = parse_json_column(kafka_df_company, schema_company)

    streaming_query_job = selection_df_job.writeStream \
            .foreachBatch(mongo_sink_job) \
            .outputMode("append") \
            .start()

    streaming_query_company = selection_df_company.writeStream \
            .foreachBatch(mongo_sink_company) \
            .outputMode("append") \
            .start()

    streaming_query_job.awaitTermination()
    streaming_query_company.awaitTermination()

if __name__ == "__main__":
    main()