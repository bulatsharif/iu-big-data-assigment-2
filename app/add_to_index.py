import os
import re
import sys
import time
from collections import Counter

from cassandra.cluster import Cluster


if len(sys.argv) != 2:
    raise SystemExit("Usage: python3 add_to_index.py <local_txt_file>")

file_path = sys.argv[1]

if not os.path.isfile(file_path):
    raise FileNotFoundError(file_path)

file_name = os.path.basename(file_path)

if not file_name.endswith(".txt"):
    raise ValueError("Input file must end with .txt")

stem = file_name[:-4]

if "_" not in stem:
    raise ValueError("Input file must match <doc_id>_<doc_title>.txt")

doc_id, doc_title = stem.split("_", 1)

with open(file_path, "r", encoding="utf-8") as handle:
    doc_text = handle.read()

tokens = re.findall(r"[A-Za-z0-9]+", doc_text.lower())
doc_len = len(tokens)

if doc_len == 0:
    raise ValueError("Document text is empty after tokenization")

term_counts = Counter(tokens)

cluster = None
session = None

for attempt in range(30):
    try:
        cluster = Cluster(["cassandra-server"])
        session = cluster.connect()
        break
    except Exception:
        time.sleep(2)

if session is None:
    raise RuntimeError("Could not connect to Cassandra")

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

document_row = session.execute(
    "SELECT doc_id FROM documents WHERE doc_id = %s",
    (doc_id,),
).one()

if document_row is not None:
    raise ValueError(f"Document already exists in index: {doc_id}")

documents_insert = session.prepare(
    "INSERT INTO documents (doc_id, title, doc_len) VALUES (?, ?, ?)"
)
postings_insert = session.prepare(
    "INSERT INTO postings (term, doc_id, tf, doc_len) VALUES (?, ?, ?, ?)"
)
vocabulary_select = session.prepare(
    "SELECT df FROM vocabulary WHERE term = ?"
)
vocabulary_insert = session.prepare(
    "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
)
stats_select = session.prepare(
    "SELECT stat_value FROM stats WHERE stat_name = ?"
)
stats_insert = session.prepare(
    "INSERT INTO stats (stat_name, stat_value) VALUES (?, ?)"
)

session.execute(documents_insert, (doc_id, doc_title, doc_len))

for term in sorted(term_counts):
    session.execute(postings_insert, (term, doc_id, term_counts[term], doc_len))

    vocabulary_row = session.execute(vocabulary_select, (term,)).one()

    if vocabulary_row is None:
        session.execute(vocabulary_insert, (term, 1))
    else:
        session.execute(vocabulary_insert, (term, int(vocabulary_row.df) + 1))

total_docs_row = session.execute(stats_select, ("total_docs",)).one()
total_doc_length_row = session.execute(stats_select, ("total_doc_length",)).one()

total_docs = float(total_docs_row.stat_value) if total_docs_row is not None else 0.0
total_doc_length = (
    float(total_doc_length_row.stat_value)
    if total_doc_length_row is not None
    else 0.0
)

total_docs += 1.0
total_doc_length += float(doc_len)
avg_doc_length = total_doc_length / total_docs

session.execute(stats_insert, ("total_docs", total_docs))
session.execute(stats_insert, ("total_doc_length", total_doc_length))
session.execute(stats_insert, ("avg_doc_length", avg_doc_length))

print(f"Indexed document: {doc_id}\t{doc_title}")

cluster.shutdown()
