About Code

Our code expects 1 command line argument i.e. path to dataset directory to monitor(in hdfs).

1. We create a spark session setting the application name   and other config. Here we also specify the executor memory, executor cpus, driver memory.
2. We create a tweet schema and define the attribute columns for it.
3. We monitor the input directory for cvs files and read it as data frame.
4.  We define query windowedCounts as group all tweets in 60 minutes interval updated every 30 mins based on interaction type(Mention, Retweet, Reply) and find the count of such groups and sort it on the basis of window range.
5. We run this query and output the results to console, we dont truncate the rows and numRows is to int max -1 which is a good enough number to ensure that all rows are output to console.
6. To exit, user needs to terminate the application as its a never ending streaming application.


Running the Code

You need to create a folder structure like below and place the source code(.scala) file in src/main/scala and the set conf file in 

root folder
|
—build-cont.sbt
|
—src/main/scala/Part-B-Question-1.scala

Then to compile go to the root folder and use command 
sbt package
 to compile the code. After compiling it will create a jar file in target/scala-2.10/ . In this case it is part-b-question-1_2.10-1.0.jar . Then to run the jar use this command
spark-submit --class "PartBQuestion1" --master spark://10.254.0.147:7077 target/scala-2.10/part-b-question-1_2.10-1.0.jar /user/ubuntu/cs-838/part-b/workload/dataset-stream/

where:
1. “PartBQuestion1” is the class/object name of the class we want to run in Part-B-Question-1.scala . 
2.  spark://10.254.0.147:7077 is the url where spark master is running. 
3. target/scala-2.10/part-b-question-1_2.10-1.0.jar is the location of our jar file
4. /user/ubuntu/cs-838/part-b/workload/dataset-stream/ is the location of directory to monitor for dataset



