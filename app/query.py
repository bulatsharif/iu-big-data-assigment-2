import math
import re
import sys
import time

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


query_text = " ".join(sys.argv[1:]).strip()

if not query_text:
    query_text = sys.stdin.read().strip()

if not query_text:
    raise SystemExit("Empty query")

query_terms = sorted(set(re.findall(r"[A-Za-z0-9]+", query_text.lower())))

if not query_terms:
    raise SystemExit("Empty query")

spark = (
    SparkSession.builder
    .appName("search query")
    .getOrCreate()
)

sc = spark.sparkContext

cluster = None
session = None

for attempt in range(30):
    try:
        cluster = Cluster(["cassandra-server"])
        session = cluster.connect("search_engine")
        break
    except Exception:
        time.sleep(2)

if session is None:
    raise RuntimeError("Could not connect to Cassandra")

stats_rows = session.execute("SELECT stat_name, stat_value FROM stats")
stats = {}

for row in stats_rows:
    stats[row.stat_name] = float(row.stat_value)

total_docs = stats.get("total_docs", 0.0)
avg_doc_length = stats.get("avg_doc_length", 0.0)

if total_docs <= 0 or avg_doc_length <= 0:
    cluster.shutdown()
    spark.stop()
    raise SystemExit("Index statistics are missing")

vocabulary_select = session.prepare(
    "SELECT df FROM vocabulary WHERE term = ?"
)
postings_select = session.prepare(
    "SELECT term, doc_id, tf, doc_len FROM postings WHERE term = ?"
)
document_select = session.prepare(
    "SELECT title FROM documents WHERE doc_id = ?"
)

df_by_term = {}
postings_rows = []

for term in query_terms:
    vocabulary_row = session.execute(vocabulary_select, (term,)).one()

    if vocabulary_row is None:
        continue

    df_by_term[term] = int(vocabulary_row.df)

    for posting_row in session.execute(postings_select, (term,)):
        postings_rows.append(
            (
                posting_row.term,
                posting_row.doc_id,
                int(posting_row.tf),
                int(posting_row.doc_len),
            )
        )

if not postings_rows:
    cluster.shutdown()
    spark.stop()
    raise SystemExit(0)

df_by_term_broadcast = sc.broadcast(df_by_term)
total_docs_broadcast = sc.broadcast(float(total_docs))
avg_doc_length_broadcast = sc.broadcast(float(avg_doc_length))
k1 = 1.5
b = 0.75

top_documents = (
    sc.parallelize(postings_rows)
    .map(
        lambda row: (
            row[1],
            math.log(total_docs_broadcast.value / df_by_term_broadcast.value[row[0]])
            * ((k1 + 1) * row[2])
            / (
                k1
                * ((1 - b) + b * row[3] / avg_doc_length_broadcast.value)
                + row[2]
            ),
        )
    )
    .reduceByKey(lambda left, right: left + right)
    .takeOrdered(10, key=lambda item: -item[1])
)

for doc_id, score in top_documents:
    document_row = session.execute(document_select, (doc_id,)).one()

    if document_row is not None:
        print(f"{doc_id}\t{document_row.title}")

cluster.shutdown()
spark.stop()
