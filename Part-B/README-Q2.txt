About Code

Our code expects 3 command line argument - 1) path to dataset directory to monitor(in hdfs), 2) Path to which output(parquet files) will be written(in hdfs), 3) Path to checkpoint directory(in hfs)

1. We create a spark session setting the application name   and other config. Here we also specify the executor memory, executor cpus, driver memory.
2. We create a tweet schema and define the attribute columns for it.
3. We monitor the input directory for cvs files and read it as data frame.
4.  We define query mtusers as all users(userIds in UserB column) s.t. they they have been mentioned(Interaction = MT) by other users(userIds in UserA col).
5. We run this query and output the results to hdfs as parquet files. We check input directory for new data every 10 secs.
6. To exit, user needs to terminate the application as its a never ending streaming application.


Running the Code

You need to create a folder structure like below and place the source code(.scala) file in src/main/scala and the set conf file in 

root folder
|
—build-cont.sbt
|
—src/main/scala/Part-B-Question-2.scala

Then to compile go to the root folder and use command 
sbt package
 to compile the code. After compiling it will create a jar file in target/scala-2.10/ . In this case it is part-b-question-2_2.10-1.0.jar . Then to run the jar use this command

spark-submit --class "PartBQuestion2" --master spark://10.254.0.147:7077 target/scala-2.10/part-b-question-2_2.10-1.0.jar /user/ubuntu/cs-838/part-b/workload/dataset-stream/ /user/ubuntu/cs-838/part-b/question-2/output /user/ubuntu/cs-838/part-b/question-2/checkpoint

where:
1. “PartBQuestion2” is the class/object name of the class we want to run in Part-B-Question-2.scala . 
2.  spark://10.254.0.147:7077 is the url where spark master is running. 
3. target/scala-2.10/part-b-question-2_2.10-1.0.jar is the location of the jar file
4. /user/ubuntu/cs-838/part-b/workload/dataset-stream/ is the location of the directory to monitor for incoming .csv files
5. /user/ubuntu/cs-838/part-b/question-2/output is the directory where we write the output as parquet files
6. user/ubuntu/cs-838/part-b/question-2/checkpoint is the directory where checkpoint is stored.



