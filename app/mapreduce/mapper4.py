import sys


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 3:
        continue

    doc_id, doc_title, doc_len = parts
    print("total_docs\t1")
    print(f"total_doc_length\t{doc_len}")
