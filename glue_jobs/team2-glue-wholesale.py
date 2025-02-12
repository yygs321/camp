import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum

# GlueContext 및 SparkContext 생성
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# S3 경로
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_OUTPUT_PATH']

# 데이터 로드
df = spark.read.csv(s3_input_path, header=True, encoding='EUC-KR')

# 임시테이블화
df.createOrReplaceTempView("wholesale")

# 데이터 정제 처리
result_df = spark.sql("""
SELECT
    saleDate,
    whsalCd,
    large,
    largeName,
    mid,
    midName,
    small,
    smallName,
    lvCd,
    lvName,
    sanName,
    totQty AS TOT_QYT,
    totAmt AS TOT_PRICE
FROM wholesale
WHERE sanName IS NOT NULL
  AND whsalName IS NOT NULL
ORDER BY saleDate
""")

# 결과 저장
result_df.write.mode("overwrite").format("parquet").option("header", "true").save(s3_output_path)
