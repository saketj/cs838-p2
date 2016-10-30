#!/bin/bash

HOME=/home/ubuntu/cs838-p2
OUTPUT_DIR=$HOME/output

bash $HOME/scripts/get-disk-network-stats.sh $OUTPUT_DIR/post-stats.out
java -cp $HOME/scripts DiskNetworkStatCalculator $OUTPUT_DIR/pre-stats.out $OUTPUT_DIR/post-stats.out > $OUTPUT_DIR/stats.out
