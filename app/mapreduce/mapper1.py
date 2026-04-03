import sys


cur_word = None
cur_doc_id = None
cur_count = 0

for l in sys.stdin:
    l = l.strip()
    if not l:
        continue
    word, doc_id, count = l.split("\t")
    count = int(count)
    if cur_word is None:
        cur_word = word
        cur_doc_id = doc_id
        cur_count = count
    else:
        if cur_word == word:
            cur_count += count
        else:
            if cur_word:
                print(f"{cur_word}\t{cur_doc_id}\t{cur_count}")
            cur_word = word
            cur_doc_id = doc_id
            cur_count = count
    if cur_word:
        print(f"{cur_word}\t{cur_doc_id}\t{cur_count}")

if cur_word == word:
    print(f"{cur_word}\t{cur_doc_id}\t{cur_count}")