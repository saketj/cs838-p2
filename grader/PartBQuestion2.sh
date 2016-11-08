#!/bin/bash
# Assumes position of streaming directory
# Rest of the stuff can be customised by changing variables below
source /home/ubuntu/run.sh
echo "This application expects apache spark, hdfs and hive metastore to be running on the cluster "
sleep 1
datasetStreamDirectory=/user/ubuntu/cs-838/part-b/workload/dataset-stream/
checkpointDirectory=/user/ubuntu/cs-838/part-b/question-2/checkpoint
outputDirectory=/user/ubuntu/cs-838/part-b/question-2/output
sparkMasterURL=spark://10.254.0.147:7077
jarFile=/home/ubuntu/cs838-p2/Part-B/proj-question-2/target/scala-2.10/part-b-question-2_2.10-1.0.jar
echo Removing checkpoint folder
hadoop fs -rm -r $checkpointDirectory
echo Creating checkpoint folder
hadoop fs -mkdir -p $checkpointDirectory
echo Removing output folder
hadoop fs -rm -r $outputDirectory
echo Creating output folder
hadoop fs -mkdir -p $outputDirectory
echo Running Spark Code
spark-submit --class "PartBQuestion2" --master $sparkMasterURL $jarFile $datasetStreamDirectory $outputDirectory $checkpointDirectory

