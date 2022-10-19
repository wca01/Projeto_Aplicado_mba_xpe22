from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

parquet_path = "s3a://mba-xpe22-processing-zone/olist_parquet"
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

    customers = spark.read.parquet(parquet_path + "/customers.parquet")
    items = spark.read.parquet(parquet_path + "/items.parquet")
    payments = spark.read.parquet(parquet_path + "/payments.parquet")
    orders = spark.read.parquet(parquet_path + "/orders.parquet")
    products = spark.read.parquet(parquet_path + "/products.parquet")
    products_cat = spark.read.parquet(parquet_path + "/products_cat.parquet")
    orders_reviews = spark.read.parquet(parquet_path + "/orders_reviews.parquet")
    sellers = spark.read.parquet(parquet_path + "/sellers.parquet")

    df_merged = orders.join(payments, on=["order_id"], how="inner")
    df_merged = df_merged.join(customers, on=["customer_id"], how="inner")
    df_merged = df_merged.join(items, on=["order_id"], how="inner")
    df_merged = df_merged.join(products, on=["product_id"], how="inner")
    df_merged = df_merged.join(sellers, on=["seller_id"], how="inner")
    df_merged = df_merged.join(products_cat, on=["product_category_name"], how="inner")

    df_merged.write.mode("overwrite").format("parquet").save("s3://mba-xpe22-delivery-zone/olist_data")

    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()