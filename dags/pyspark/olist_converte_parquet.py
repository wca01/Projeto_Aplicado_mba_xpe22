from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()
    

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .config('spark.executor.memory', '8G')\
            .appName("OLIST Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    olist_path = "s3a://mba-xpe22-landing-zone/olist/"
    parquet_path = "s3a://mba-xpe22-processing-zone/olist_parquet/"

    df_customers = spark.read.format("csv").options(header=True, inferSchema=True, delimiter=';') \
    .load(f"{olist_path}/olist_customers_dataset.csv")
    df_customers.printSchema()

    df_orders = spark.read.format("csv") \
                .options(header=True, inferSchema=True, timestampFormat="y-M-d H:m:s") \
                .load(f"{olist_path}/olist_orders_dataset.csv")
    df_orders.printSchema()

    df_order_items = spark.read.format("csv") \
                      .options(header=True, inferSchema=True, timestampFormat="y-M-d H:m:s") \
                      .load(f"{olist_path}/olist_order_items_dataset.csv")
    df_order_items.printSchema()

    df_payments = spark.read.format("csv") \
                   .options(header=True, inferSchema=True) \
                   .load(f"{olist_path}/olist_order_payments_dataset.csv")
    df_payments.printSchema()

    df_products = spark.read.format("csv") \
                   .options(header=True, inferSchema=True) \
                   .load(f"{olist_path}/olist_products_dataset.csv")
    df_products.printSchema()

    df_sellers = spark.read.format("csv") \
                  .options(header=True, inferSchema=True) \
                  .load(f"{olist_path}/olist_sellers_dataset.csv")
    df_sellers.printSchema()

    df_product_category = spark.read.format("csv") \
                           .options(header=True, inferSchema=True) \
                           .load(f"{olist_path}/product_category_name_translation.csv")
    df_product_category.printSchema()
    
    df_orders_reviews = spark.read.format("csv") \
                   .options(header=True, inferSchema=True) \
                   .load(f"{olist_path}/olist_order_reviews_dataset.csv")
    df_orders_reviews.printSchema()

    df_customers.write.parquet(parquet_path + "/customers.parquet")
    df_orders.write.parquet(parquet_path + "/orders.parquet")
    df_order_items.write.parquet(parquet_path + "/items.parquet")
    df_payments.write.parquet(parquet_path + "/payments.parquet")
    df_products.write.parquet(parquet_path + "/products.parquet")
    df_sellers.write.parquet(parquet_path + "/sellers.parquet")
    df_product_category.write.parquet(parquet_path + "/products_cat.parquet")
    df_orders_reviews.write.parquet(parquet_path + "/orders_reviews.parquet")

print("*********************")
print("Escrito com sucesso!")
print("*********************")

spark.stop()