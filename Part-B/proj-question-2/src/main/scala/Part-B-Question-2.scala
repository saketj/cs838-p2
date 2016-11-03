import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.DataFrameWriter

object PartBQuestion2 {
	def main(args: Array[String]) {
	val inputDirectory = args(0)
	val outputDirectory = args(1)
	val checkpointDirectory = args(2)
	val spark = SparkSession
	    .builder
	    .appName("CS-838-Assignment2-PartB-2")
	    .config("spark.eventLog.enabled","true")
	    .config("spark.eventLog.dir","hdfs:///tmp/spark-events")
	    .config("spark.driver.memory","1g")
	    .config("spark.executor.cores","4")
	    .config("spark.task.cpus","1")
	    .config("spark.sql.streaming.checkpointLocation", checkpointDirectory)
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
	    .csv(inputDirectory)
	val mtUsers = df.select("userB")
	    .where("interaction = 'MT'")
	val query = mtUsers.writeStream
	    .format("parquet")	    
	    .trigger(ProcessingTime.create("10 seconds"))
	    .option("truncate", "false")
	    .option("numRows", 2147483646)
	    .start(outputDirectory)
	query.awaitTermination()
	}
}
