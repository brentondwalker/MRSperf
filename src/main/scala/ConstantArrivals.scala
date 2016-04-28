import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.math.random

object ConstantArrivals {
  
  def runPiSlices(spark:SparkContext, n: Int, slices:Int): Long = {
    println("*** runPiSlices( "+n+" , "+slices+" )")
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    return count
  }
  
	def main(args: Array[String]) {
		val conf = new SparkConf().setMaster("local[1]").setAppName("Spark Pi")
		println("*** got conf ***")
		val spark = new SparkContext(conf)
		println("*** got spark context ***")
		val totalSlices = if (args.length > 0) args(0).toInt else 2
		println("*** totalSlices = "+totalSlices+" ***")
		val totalN = math.min(100000L * totalSlices, Int.MaxValue).toInt
		val slicesPerStep = 2
		val iterationsPerSlice = totalN/totalSlices
		var count: Long = 0
		var slicesRun = 0
		while (slicesRun < totalSlices) {
		  var s = math.min(slicesPerStep, totalSlices-slicesRun)
		  count += runPiSlices(spark, iterationsPerSlice*s, s)
		  slicesRun += s
		  Thread sleep 1000
		}
		println("Pi is roughly " + 4.0 * count / totalN)
    spark.stop()
	}
}


