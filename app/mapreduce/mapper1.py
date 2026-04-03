import re
import sys
from collections import Counter


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, doc_title, doc_text = parts
    tokens = re.findall(r"[A-Za-z0-9]+", doc_text.lower())
    doc_len = len(tokens)

    if doc_len == 0:
        continue

    term_counts = Counter(tokens)

    for term in sorted(term_counts):
        print(f"{term}\t{doc_id}\t{term_counts[term]}\t{doc_len}")
