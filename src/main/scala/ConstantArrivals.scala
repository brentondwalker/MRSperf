import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.concurrent.CountDownLatch

import scala.math.random

object ConstantArrivals {
  
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
	  //val conf = new SparkConf().setMaster("local[1]").setAppName("ConstantArrivals")
	  val conf = new SparkConf().setAppName("ConstantArrivals")
	  println("*** got conf ***")
		val spark = new SparkContext(conf)
		println("*** got spark context ***")
		val totalSlices = if (args.length > 0) args(0).toInt else 2
		val slicesPerStep = if (args.length > 1) args(1).toInt else 1
		var totalJobs = totalSlices/slicesPerStep
		if ((totalSlices%slicesPerStep) > 0) {
		  totalJobs += 1
		}
		println("*** totalSlices = "+totalSlices+" ***")
		println("*** slicesPerStep = "+slicesPerStep+" ***")
  	println("*** totalJobs = "+totalJobs+" ***")
		var slicesRun = 0
		var doneSignal: CountDownLatch = new CountDownLatch(totalJobs);
		val initialTime = java.lang.System.currentTimeMillis()
		while (slicesRun < totalSlices) {
			var s = math.min(slicesPerStep, (totalSlices - slicesRun))
					val t = new Thread(new Runnable {
						def run() {
						  val startTime = java.lang.System.currentTimeMillis()
						  println("+++ START: "+startTime)
							runEmptySlices(spark, s)
							val stopTime = java.lang.System.currentTimeMillis()
							println("--- STOP: "+stopTime)
							println("=== ELAPSED: "+(stopTime-startTime))
							println("=== TOTAL ELAPSED: "+(stopTime-initialTime))
							doneSignal.countDown()
						}
					})
					slicesRun += s
					t.start()
					//Thread sleep 5000
		}
	  println("*** FINISHED!! ***")
	  doneSignal.await()
	  spark.stop()
  }
}


