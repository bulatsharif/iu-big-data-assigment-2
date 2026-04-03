import sys


current_stat = None
current_value = 0
total_docs = 0
total_doc_length = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    stat_name, value = parts
    value = int(value)

    if current_stat is None:
        current_stat = stat_name
        current_value = value
    elif stat_name == current_stat:
        current_value += value
    else:
        if current_stat == "total_docs":
            total_docs = current_value
        elif current_stat == "total_doc_length":
            total_doc_length = current_value

        current_stat = stat_name
        current_value = value

if current_stat == "total_docs":
    total_docs = current_value
elif current_stat == "total_doc_length":
    total_doc_length = current_value

if total_docs > 0:
    avg_doc_length = total_doc_length / total_docs
    print(f"total_docs\t{total_docs}")
    print(f"total_doc_length\t{total_doc_length}")
    print(f"avg_doc_length\t{avg_doc_length:.6f}")
