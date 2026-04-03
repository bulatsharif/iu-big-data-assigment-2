import sys


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    term, doc_id, tf, doc_len = parts
    print(f"{term}\t1")
