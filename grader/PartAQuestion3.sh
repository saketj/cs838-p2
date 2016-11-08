#!/bin/bash
# Rest of the stuff can be customised by changing variables below
#Assumes data is present in HDFS to which datasetOriginal points to
dataset=hdfs:///user/ubuntu/cs-838/part-a/workload/web-BerkStan.txt
sparkMasterURL=spark://10.254.0.147:7077
numberOfIterations=10
numberOfPartitions=40
jarFile=/home/ubuntu/cs838-p2/Part-A/sbt-proj-question-3/target/scala-2.10/part-a-question-3_2.10-1.0.jar
echo Running Spark Code
spark-submit --class "PartAQuestion3" --master $sparkMasterURL $jarFile $dataset $numberOfPartitions $numberOfIterations > PartAQuestion3Output.txt
cat PartAQuestion3Output.txt
