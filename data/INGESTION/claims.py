from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

spark = SparkSession.builder.appName("Healthcare Claims Ingestion").getOrCreate()

BUCKET_NAME = "healthcare-bucket-01022026"
CLAIMS_BUCKET_PATH = "gs://healthcare-bucket-01022026/landing/claims/*.csv"
BQ_TABLE = "project-6f02684e-1375-4020-964.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

claims_df = spark.read.csv(CLAIMS_BUCKET_PATH,header=True)

claims_df = (claims_df.withColumn("datasource", when(input_file_name().contains("hospital1"),"hosa")
                                   .when(input_file_name().contains("hospital2"),"hosb")
                                   .otherwise("None")
                     ))
(claims_df.write.format('bigquery').option('table',BQ_TABLE)
        .option("temporaryGcsBucket",TEMP_GCS_BUCKET).mode("overwrite").save())