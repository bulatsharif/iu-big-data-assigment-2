import subprocess
import time

from cassandra.cluster import Cluster


cluster = Cluster(["cassandra-server"])
session = cluster.connect()

session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """
)

session.set_keyspace("search_engine")

session.execute(
    """
    CREATE TABLE IF NOT EXISTS postings (
        term text,
        doc_id text,
        tf int,
        doc_len int,
        PRIMARY KEY (term, doc_id)
    )
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        df int
    )
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS documents (
        doc_id text PRIMARY KEY,
        title text,
        doc_len int
    )
    """
)

session.execute(
    """
    CREATE TABLE IF NOT EXISTS stats (
        stat_name text PRIMARY KEY,
        stat_value double
    )
    """
)

session.execute("TRUNCATE postings")
session.execute("TRUNCATE vocabulary")
session.execute("TRUNCATE documents")
session.execute("TRUNCATE stats")

postings_insert = session.prepare(
    "INSERT INTO postings (term, doc_id, tf, doc_len) VALUES (?, ?, ?, ?)"
)
vocabulary_insert = session.prepare(
    "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
)
documents_insert = session.prepare(
    "INSERT INTO documents (doc_id, title, doc_len) VALUES (?, ?, ?)"
)
stats_insert = session.prepare(
    "INSERT INTO stats (stat_name, stat_value) VALUES (?, ?)"
)

index_lines = subprocess.run(
    "hdfs dfs -cat /indexer/index/part-*",
    shell=True,
    executable="/bin/bash",
    check=True,
    capture_output=True,
    text=True,
).stdout.splitlines()

vocabulary_lines = subprocess.run(
    "hdfs dfs -cat /indexer/vocabulary/part-*",
    shell=True,
    executable="/bin/bash",
    check=True,
    capture_output=True,
    text=True,
).stdout.splitlines()

documents_lines = subprocess.run(
    "hdfs dfs -cat /indexer/documents/part-*",
    shell=True,
    executable="/bin/bash",
    check=True,
    capture_output=True,
    text=True,
).stdout.splitlines()

stats_lines = subprocess.run(
    "hdfs dfs -cat /indexer/stats/part-*",
    shell=True,
    executable="/bin/bash",
    check=True,
    capture_output=True,
    text=True,
).stdout.splitlines()

for line in index_lines:
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    term, doc_id, tf, doc_len = parts
    session.execute(postings_insert, (term, doc_id, int(tf), int(doc_len)))

for line in vocabulary_lines:
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    term, df = parts
    session.execute(vocabulary_insert, (term, int(df)))

for line in documents_lines:
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 3:
        continue

    doc_id, title, doc_len = parts
    session.execute(documents_insert, (doc_id, title, int(doc_len)))

for line in stats_lines:
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    stat_name, stat_value = parts
    session.execute(stats_insert, (stat_name, float(stat_value)))

print(f"Loaded postings: {len(index_lines)}")
print(f"Loaded vocabulary rows: {len(vocabulary_lines)}")
print(f"Loaded documents: {len(documents_lines)}")
print(f"Loaded stats rows: {len(stats_lines)}")

cluster.shutdown()
