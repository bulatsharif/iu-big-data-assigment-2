import sys


for line in sys.stdin:
    line = line.rstrip("\n")
    if line:
        print(line)
