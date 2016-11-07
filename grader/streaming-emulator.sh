#!/bin/bash
# Below code assumes that dataset is csv files named 1.csv to 1027.csv and is stored in hdfs:///user/ubuntu/cs-838/part-b/workload/dataset/
# Rest of the stuff can be customised by changing variables below

#Assumes data is present in HDFS to which datasetOriginal points to
datasetOriginal=/user/ubuntu/cs-838/part-b/workload/dataset/
datasetCopy=/user/ubuntu/cs-838/part-b/workload/dataset-copy/
datasetStreamDirectory=/user/ubuntu/cs-838/part-b/workload/dataset-stream/
sleepTime=5s

echo Removing files from dataset-copy
hadoop fs -rm -r $datasetCopy*
echo Copying data into a temporary folder before starting streaming
#hadoop fs -mkdir -p $datasetCopy
hadoop fs -cp $datasetOriginal*.csv $datasetCopy
echo Removing files old stream dataset folder
hadoop fs -rm -r $datasetStreamDirectory*
#echo Creating folder for stream dataset
#hadoop fs -mkdir -p $datasetStreamDirectory

echo Moving data for streaming
for i in {1..1027}
do      
        j=$datasetCopy$i.csv
        echo streaming $j 
        hadoop fs -mv $j $datasetStreamDirectory/$i.csv 
        sleep $sleepTime
        echo $j 
done

