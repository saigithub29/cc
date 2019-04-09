#!/usr/bin/python3

import sys

def category_mapper():
    line_count = 0
    for row in sys.stdin:
	row = line.strip().split(",")
        if line_count == 0:
            line_count += 1
        else:
            if len(row) != 12:
                continue
            video_id = row[0].strip()
            category = row[3].strip()
            country = row[11].strip()
            print("{}\t{}\t{}".format(category, video_id, country))

if __name__ == "__main__":
    category_mapper()
