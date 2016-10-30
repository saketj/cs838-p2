import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PartBQuestion1 {
	def main(args: Array[String]) {
	val directory = args(0)
	val spark = SparkSession
	    .builder
	    .appName("CS-838-Assignment2-PartB-1")
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
	val windowedCounts = df.groupBy(
	    		   window($"timestamp", "60 minutes", "30 minutes")
	    		   , $"interaction")
		.count()
		.orderBy("window")
	val query = windowedCounts.writeStream
	    .queryName("tweets")
	    .outputMode("complete")
	    .format("console")
	    .option("truncate", "false")
	    .option("numRows", 2147483646)
	    .start()
	query.awaitTermination()
	}
}
