import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime

object PartBQuestion3 {
	def main(args: Array[String]) {
	val directory = args(0)
	val userListFile = args(1)
	val spark = SparkSession
	    .builder
	    .appName("CS-838-Assignment2-PartB-3")
	    .config("spark.eventLog.enabled","true")
	    .config("spark.eventLog.dir","hdfs:///tmp/spark-events")
	    .config("spark.driver.memory","1g")
	    .config("spark.executor.cores","4")
	    .config("spark.task.cpus","1")
	    .getOrCreate()
	import spark.implicits._
	val tweetSchema = new StructType()
	    .add("userA", "string")
	    .add("userB", "string")
	    .add("timestamp", "string")
	    .add("interaction", "string")
	val df = spark.readStream
	    .option("sep", ",")
	    .schema(tweetSchema)
	    .csv(directory)
	val userList = spark.sparkContext
	    .textFile(userListFile)
	    .map(_.split(","))
	    .first()
	val filteredSchema = df.filter(col("userA")
	    .isin(userList:_*))
            .select("userA", "interaction")
	    .groupBy("userA")
	    .count()
	val query = filteredSchema.writeStream
	    .outputMode("complete")
	    .format("console")
	    .trigger(ProcessingTime.create("5 seconds"))
	    .option("truncate", "false")
	    .option("numRows", 2147483646)
	    .start()
	query.awaitTermination()
	}
}
