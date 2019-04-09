#!/bin/bash

spark-submit \
    --master local[4] \
    TopTenVideos.py \
    --input file:///home/hadoop/assignment1/workload2/ \
    --output file:///home/hadoop/viedos_output/
	 

    
