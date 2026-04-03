import sys
import re

for l in sys.stdin:
    l = l.strip()
    if not l:
        continue
    parts = l.split("\t")
    if len(parts) != 3:
        continue
    doc_id, doc_title, doc_text = parts
    words = re.findall(r'\b\w+\b', doc_text.lower())
    
    for word in words:
        print(f"{word}\t{doc_id}\t{doc_title}")