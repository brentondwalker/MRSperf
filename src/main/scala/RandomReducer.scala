import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.concurrent.CountDownLatch
import org.apache.spark.scheduler
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler._
import scala.util.Random

import scala.math.random

object RandomReducer {
  

  /**
   * sbt "run-main RandomReducer 4 10000"
   * sbt "run-main RandomReducer 4 100000 1000"
   */
  def main(args: Array[String]) {
	  val conf = new SparkConf().setMaster("local[2]").setAppName("RandomReducer")
	  //val conf = new SparkConf().setAppName("RandomReducer")
	  println("*** got conf ***")
		val spark = new SparkContext(conf)
		println("*** got spark context ***")
		
		
		// give the system a little time for the executors to start... 30 seconds?
		// this is stupid b/c the full set of executors actually take less tha 1s to start
		print("Waiting a moment to let the executors start...")
		Thread sleep 1000*5
		println("done waiting!\n")
		
	  val numPartitions = if (args.length > 0) args(0).toInt else 1
	  val recordsPerPartition = if (args.length > 1) args(1).toInt else 1000000
	  val numKeys = if (args.length > 2) args(2).toInt else 1000
	  
		val distData = spark.parallelize(Seq[Int](), numPartitions)
  		.mapPartitions { _ => {
	  		(1 to recordsPerPartition).map{_ => (Random.nextInt(numKeys), 1)}.iterator
		  }}
	  //distData.take(100).foreach(println)

	  // force the data to be computed and cached across the cluster
	  distData.persist()
	  
	  println("reducing by key...")
	  val summedData = distData.reduceByKey((a,b) => a + b)
	  //summedData.take(100).foreach(println)
	  println(summedData.collect())
	  
	  //println("making 
	  //distData.map(x => (Random.nextInt(numKeys), x))
	  
	  
		println("*** FINISHED!! ***")
		
		Thread sleep 1000*5

		println("*** stopping spark ***")
		spark.stop()
  }
}


