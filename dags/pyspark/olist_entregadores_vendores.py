from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

parquet_path = "s3://mba-xpe22-processing-zone/olist_parquet"
delivery_zone = "s3://mba-xpe22-delivery-zone/olist_data"

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2')
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

    merged = spark.read.parquet(parquet_path + "/merged.parquet")
    df_cons_sells = merged.select('order_id','customer_id', 'customer_city',
 'customer_state','seller_id','seller_city','seller_state')
    df_cons_sells.toPandas().to_csv(delivery_zone+'/df_cons_sells.csv')

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()