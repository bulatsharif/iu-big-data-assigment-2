import re
import sys


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

    print(f"{doc_id}\t{doc_title}\t{doc_len}")
