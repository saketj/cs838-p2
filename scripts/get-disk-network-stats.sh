#!/bin/bash

OUTPUT=$1

#Getting the disk stats
ssh ubuntu@vm-27-1 'cat /proc/diskstats' | tail -2  > $OUTPUT
ssh ubuntu@vm-27-2 'cat /proc/diskstats' | tail -2  >> $OUTPUT
ssh ubuntu@vm-27-3 'cat /proc/diskstats' | tail -2  >> $OUTPUT
ssh ubuntu@vm-27-4 'cat /proc/diskstats' | tail -2  >> $OUTPUT
ssh ubuntu@vm-27-5 'cat /proc/diskstats' | tail -2  >> $OUTPUT

#Getting the network stats
ssh ubuntu@vm-27-1 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
ssh ubuntu@vm-27-2 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
ssh ubuntu@vm-27-3 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
ssh ubuntu@vm-27-4 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
ssh ubuntu@vm-27-5 'cat /proc/net/dev' | tail -2 | head -1 >> $OUTPUT
