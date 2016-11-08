#!/bin/bash
# Assumes position of streaming directory 
# Rest can be customised by changing variables below
source /home/ubuntu/run.sh
echo "This application expects apache spark, hdfs and hive metastore to be running on the cluster"
sleep 1
datasetStreamDirectory=/user/ubuntu/cs-838/part-b/workload/dataset-stream/
sparkMasterURL=spark://10.254.0.147:7077
jarFile=/home/ubuntu/cs838-p2/Part-B/proj-question-1/target/scala-2.10/part-b-question-1_2.10-1.0.jar
echo Running Spark Code
spark-submit --class "PartBQuestion1" --master $sparkMasterURL $jarFile $datasetStreamDirectory

