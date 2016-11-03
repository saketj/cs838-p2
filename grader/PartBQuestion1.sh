#!/bin/bash
# Below code assumes that dataset is csv files named 1.csv to 1027.csv and is stored in hdfs:///user/ubuntu/cs-838/part-b/workload/dataset/
# Rest of the stuff can be customised by changing variables below
function copyFiles() {
	echo Moving data for streaming
	for i in {1..1027}
	do
		j=$datasetCopy$i.csv
		echo streaming $j 
		hadoop fs -mv $j $datasetStreamDirectory/$i.csv 
		sleep $sleepTime
		echo $j
	done
}

#Assumes data is present in HDFS to which datasetOriginal points to
datasetOriginal=/user/ubuntu/cs-838/part-b/workload/dataset/
datasetCopy=/user/ubuntu/cs-838/part-b/workload/dataset-copy/
datasetStreamDirectory=/user/ubuntu/cs-838/part-b/workload/dataset-stream/
sleepTime=5s
sparkMasterURL=spark://10.254.0.147:7077
jarFile=/home/ubuntu/cs838-p2/Part-B/proj-question-1/target/scala-2.10/part-b-question-1_2.10-1.0.jar
echo Removing dataset-copy
hadoop fs -rm -r $datasetCopy
echo Creating copy
hadoop fs -mkdir -p $datasetCopy
hadoop fs -cp $datasetOriginal*.csv $datasetCopy
echo Removing old stream dataset folder
hadoop fs -rm -r $datasetStreamDirectory
echo Creating folder for stream dataset
hadoop fs -mkdir -p $datasetStreamDirectory
copyFiles &
echo Running Spark Code
spark-submit --class "PartBQuestion1" --master $sparkMasterURL $jarFile $datasetStreamDirectory

