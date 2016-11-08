About Code

Our code expects 2 command line arguments - 1) path to dataset directory to monitor(in hdfs), 2) Path to list of interested users(comma separated list in hfs)

1. We create a spark session setting the application name   and other config. Here we also specify the executor memory, executor cpus, driver memory.
2. We create a tweet schema and define the attribute columns for it.
3. We monitor the input directory for cvs files and read it as data frame.
4. We parse the list of interested users.
5.  We define query filteredSchema by filtering users A s.t. they belong to our list of interested users and find the number of retweets or replies by them.
6. We run this query and output the results to console, we check input directory for new data every 5 secs.
7. To exit, user needs to terminate the application as its a never ending streaming application.


Running the Code

You need to create a folder structure like below and place the source code(.scala) file in src/main/scala and the set conf file in 

root folder
|
—build-cont.sbt
|
—src/main/scala/Part-B-Question-3.scala

Then to compile go to the root folder and use command 
sbt package
 to compile the code. After compiling it will create a jar file in target/scala-2.10/ . In this case it is part-b-question-3_2.10-1.0.jar . Then to run the jar use this command

spark-submit --class "PartBQuestion3" --master spark://10.254.0.147:7077 target/scala-2.10/part-b-question-3_2.10-1.0.jar /user/ubuntu/cs-838/part-b/workload/dataset-stream/ /user/ubuntu/cs-838/part-b/question-3/user-list.csv

where:
1. “PartBQuestion3” is the class/object name of the class we want to run in Part-B-Question-3.scala . 
2.  spark://10.254.0.147:7077 is the url where spark master is running. 
3. target/scala-2.10/part-b-question-3_2.10-1.0.jar is the location of the jar file
4. /user/ubuntu/cs-838/part-b/workload/dataset-stream/ is the location of the directory to monitor for incoming .csv files
5. /user/ubuntu/cs-838/part-b/question-3/user-list.csv is the file in which comma separated list of interested users are present

