
import org.apache.spark.sql.SparkSession
import org.apache.spark.RangePartitioner

object PartAQuestion2 {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("The program expects at least one argument- the file location and number of iterations (optional)")
      System.exit(1)
    }

    val sparkSession = SparkSession
      .builder
      .appName("CS-838-Assignment2-PartA-2")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "hdfs:///tmp/spark-events")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "1g")
      .config("spark.executor.cores", "4")
      .config("spark.task.cpus", "1")
      .getOrCreate()
    
    val num_iterations = if (args.length > 1) args(1).toInt else 10  // Default number of iterations is 10.
    val num_partitions = 40  // Number of partitions is set to 40

    // Load the dataset into a partitioned RDD assuming that the input data is uncompressed.
    val lines = sparkSession.sparkContext.textFile(args(0), num_partitions)  
										     
    val rawLinks = lines.map{ line =>
      val urls = line.split("\\s+")
      (urls(0), urls(1))
    }

    val customPartitioner = new RangePartitioner(num_partitions, rawLinks)

    val links = rawLinks.groupByKey(customPartitioner)

    var ranks = links.mapValues(rank => 1.0)  // Initial rank is set to 1.0.

    for (i <- 1 to num_iterations) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }.partitionBy(customPartitioner)

      ranks = contribs.reduceByKey(customPartitioner, (a,b) => a + b).mapValues(sum => 0.15 + 0.85 * sum)  // Update rank.
    }

    val results = ranks.collect()
    results.foreach(result => println("url: " + result._1 + " rank: " + result._2))

    sparkSession.stop()
  }
}
