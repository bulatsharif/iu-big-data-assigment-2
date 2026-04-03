import sys


current_term = None
current_df = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    term, count = parts
    count = int(count)

    if current_term is None:
        current_term = term
        current_df = count
    elif term == current_term:
        current_df += count
    else:
        print(f"{current_term}\t{current_df}")
        current_term = term
        current_df = count

if current_term is not None:
    print(f"{current_term}\t{current_df}")
