#!/bin/bash

#Drop caches

HOME=/home/ubuntu/cs838-p2
OUTPUT_DIR=$HOME/output

ssh ubuntu@vm-27-1 'sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh ubuntu@vm-27-2 'sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh ubuntu@vm-27-3 'sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh ubuntu@vm-27-4 'sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'
ssh ubuntu@vm-27-5 'sudo -S sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'

bash $HOME/scripts/get-disk-network-stats.sh $OUTPUT_DIR/pre-stats.out 
