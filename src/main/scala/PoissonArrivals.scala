import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.concurrent.CountDownLatch
import org.apache.spark.scheduler
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler._

import scala.math.random

object PoissonArrivals {
  
  def runEmptySlices(spark:SparkContext, slices:Int): Long = {
    println("*** runEmptySlices( "+slices+" )")
    val count = spark.parallelize(1 to slices, slices).map { i =>
      val jobLength = 1
      val startTime = java.lang.System.currentTimeMillis()
      val targetStopTime = startTime + 1000*jobLength
			println("    +++ START: "+startTime)
			while (java.lang.System.currentTimeMillis() < targetStopTime) {
				val x = random * 2 - 1
				val y = random * 2 - 1
			}
      //Thread sleep 1000
      val stopTime = java.lang.System.currentTimeMillis()
      println("    --- STOP: "+stopTime)
      println("    === ELAPSED: "+(stopTime-startTime))
      1
    }.count()
    return 0
  }
  
  def main(args: Array[String]) {
	  //val conf = new SparkConf().setMaster("local[1]").setAppName("PoissonArrivals")
	  val conf = new SparkConf().setAppName("PoissonArrivals")
	  println("*** got conf ***")
		val spark = new SparkContext(conf)
		println("*** got spark context ***")
		
		//val scheduler: TaskSchedulerImpl = 
		//spark.taskScheduler
		//spark.getConf
		//println("*** got taskScheduler ***")
		//println(scheduler)
		
		val totalSlices = if (args.length > 0) args(0).toInt else 2
		val slicesPerStep = if (args.length > 1) args(1).toInt else 1
		val rate = if (args.length > 2) args(2).toDouble else 0.2
		var totalJobs = totalSlices/slicesPerStep
		if ((totalSlices%slicesPerStep) > 0) {
		  totalJobs += 1
		}
		println("*** totalSlices = "+totalSlices+" ***")
		println("*** slicesPerStep = "+slicesPerStep+" ***")
  	println("*** totalJobs = "+totalJobs+" ***")
		var slicesRun = 0
		var doneSignal: CountDownLatch = new CountDownLatch(totalJobs)
		val initialTime = java.lang.System.currentTimeMillis()
		while (slicesRun < totalSlices) {
		  println("")
			var s = math.min(slicesPerStep, (totalSlices - slicesRun));
			val t = new Thread(new Runnable {
				def run() {
					val startTime = java.lang.System.currentTimeMillis();
					println("+++ START: "+startTime)
					runEmptySlices(spark, s)
					val stopTime = java.lang.System.currentTimeMillis()
					println("--- STOP: "+stopTime)
					println("=== ELAPSED: "+(stopTime-startTime))
					println("=== TOTAL ELAPSED: "+(1.0*(stopTime-initialTime)/1000.0))
					doneSignal.countDown()
				}
			})
			slicesRun += s
			t.start()
			
			val interarrivalTime = -math.log(random)/rate
			println("*** inter-arrival time: "+interarrivalTime+" ***")
			Thread sleep math.round(interarrivalTime * 1000)
			
		}
	  println("*** FINISHED!! ***")
	  doneSignal.await()
	  spark.stop()
  }
}


