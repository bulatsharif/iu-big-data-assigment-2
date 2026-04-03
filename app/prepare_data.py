from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession, length, col


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
df = df.filter(df['title'].isNotNull())
df = df.filter(df['text'].isNotNull())
df = df.filter(df['id'].isNotNull())
df = df.filter(
    (length(col("text")) > 0) &
    ~(
        (col("text").startswith("{") & col("text").endswith("}")) |
        (col("text").startswith("[") & col("text").endswith("]")) |
        (col("text").startswith("<") & col("text").endswith(">"))
    )
).drop("text")
n = 100
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)


df.write.csv("/index/data", sep = "\t")