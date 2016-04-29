import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.math.random

object ConstantArrivalsMultiApp {
  
  def runPiSlices(spark:SparkContext, n: Int, slices:Int): Long = {
    println("*** runPiSlices( "+n+" , "+slices+" )")
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      Thread sleep 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    return count
  }
  
	def main(args: Array[String]) {
		//val conf = new SparkConf().setMaster("local[1]").setAppName("ConstantArrivalsMultiApp")
	  val conf = new SparkConf()
			  .setAppName("ConstantArrivalsMultiApp")
		println("*** got conf ***")
		val spark = new SparkContext(conf)
		println("*** got spark context ***")
		val totalSlices = if (args.length > 0) args(0).toInt else 2
		val slicesPerStep = if (args.length > 1) args(1).toInt else 1
		println("*** totalSlices = "+totalSlices+" ***")
		println("*** slicesPerStep = "+slicesPerStep+" ***")
		val totalN = math.min(100000L * totalSlices, Int.MaxValue).toInt
		val iterationsPerSlice = totalN/totalSlices
		var count: Long = 0
		var slicesRun = 0
		while (slicesRun < totalSlices) {
		  var s = math.min(slicesPerStep, totalSlices-slicesRun)
		  count += runPiSlices(spark, iterationsPerSlice*s, s)
		  slicesRun += s
		  Thread sleep 5000
		}
		println("Pi is roughly " + 4.0 * count / totalN)
    spark.stop()
	}
}


