from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F  

parquet_path = "s3://mba-xpe22-processing-zone/olist_parquet"


# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()
    

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("OLIST Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df_customers = (
        spark
        .read
        .format("parquet")
        .load(parquet_path + "/customers.parquet")
    )
df_customers.na.drop(subset=['customer_unique_id'])

df_orders = (
        spark
        .read
        .format("parquet")
        .load(parquet_path + "/orders.parquet")
    )
df_orders = df_orders.withColumn('order_purchase_timestamp', F.to_timestamp('order_purchase_timestamp')).withColumn('order_delivered_carrier_data', F.to_timestamp('order_delivered_carrier_date')).withColumn('order_approved_at',F.to_timestamp('order_approved_at')).withColumn('order_delivered_customer_date',F.to_timestamp('order_delivered_customer_date')).withColumn('order_estimed_delivery_date',F.to_timestamp('order_estimated_delivery_date'))

df_customers.write.mode('append').parquet(parquet_path + "/customers.parquet")

df_orders.write.mode('append').parquet(parquet_path + "/orders.parquet")

print("*********************")
print("Escrito com sucesso!")
print("*********************")

spark.stop()