import os

from pyspark.sql import SparkSession



def to_input_row(record):
    path, content = record
    filename = os.path.basename(path)

    if not filename.endswith(".txt"):
        return None

    stem = filename[:-4]
    if "_" not in stem:
        return None

    doc_id, doc_title = stem.split("_", 1)
    doc_text = " ".join(content.split())

    if not doc_text:
        return None

    return f"{doc_id}\t{doc_title}\t{doc_text}"


spark = (
    SparkSession.builder
    .appName("data preparation")
    .master("local")
    .getOrCreate()
)

documents = (
    spark.sparkContext
    .wholeTextFiles("hdfs:///data/*.txt")
    .map(to_input_row)
    .filter(lambda row: row is not None)
    .coalesce(1)
)

documents.saveAsTextFile("hdfs:///input/data")

spark.stop()
