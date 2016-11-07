About Code

Our code expects at-least 2 command line arguments:- 1) Path to dataset file, 2) number of partitions and an optional argument 3) number of iterations for which page rank algorithm should be run. By default its run for 10 iterations.
1. We create a spark session setting the application name   and other config. Here we also specify the executor memory, executor cpus, driver memory.  
2. Load the data into partitioned RDD(links). We assume that its an uncompressed file and not an archive.
3. Create rawLinks RDD from lines RDD by splitting the url into src,dest
4. From this rawLinks RDD we initialize a custom range partitioner, which we use later to partition links and ranks to avoid wide dependencies.
5. links RDD is created from rawLinks using the same customPartitioner and it is also cached. Caching it ensures that incase pages need to be swapped for memory, partitions of this red are last to be swapped out. This is done to decrease Disk I/O as this RDD is a dependency for all stages as can be seen from the DAG of this application.
6. Create ranks RDD from initial links RDD. We set rank for all urls to 1 initially.
7. Then we run the page rank algorithm for some iterations (by default 10).  Calculate contribution of different urls to ranks  and then updating rank accordingly.
8. We collect the ranks of all urls and output it.


Running the Code

You need to create a folder structure like below and place the source code(.scala) file in src/main/scala and the set conf file in 

root folder
|
—build-cont.sbt
|
—src/main/scala/Part-A-Question-3.scala

Then to compile go to the root folder and use command 
sbt package
 to compile the code. After compiling it will create a jar file in target/scala-2.10/ . In this case it is part-a-question-2_3.10-1.0.jar . Then to run the jar use this command
spark-submit --class "PartAQuestion3” --master spark://10.254.0.147:7077 target/scala-2.10/part-a-question-3_2.10-1.0.jar hdfs:///user/ubuntu/cs-838/part-a/workload/web-BerkStan.txt 40 10

where:
1. “PartAQuestion3” is the class name of the class we want to run in Part-A-Question-3.scala . 
2.  spark://10.254.0.147:7077 is the url where spark master is running. 
3. target/scala-2.10/part-a-question-2_3.10-1.0.jar is the location of our jar file
4. hdfs:///user/ubuntu/cs-838/part-a/workload/web-BerkStan.txt is the location of dataset file
5. 40 is the number of partitions
6. 10 is the number of iterations



