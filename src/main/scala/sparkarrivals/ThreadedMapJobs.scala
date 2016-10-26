package sparkarrivals;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.concurrent.CountDownLatch
import org.apache.spark.scheduler
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler._

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import scala.math.random


object ThreaddedMapJobs {
  
  
  /**
   * Parse command line arguments using commons-cli 1.2
   * We have to use 1.2 even though it clunky in scala, because it is used
   * in Spark.  If we try to use 1.3 or later the packages will conflilct.
   * 
   */
  def parseArgs(args: Array[String]): CommandLine = {
    val cli_options: Options = new Options();
		cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("w", "numworkers", true, "number of workers/servers");
		cli_options.addOption("t", "numtasks", true, "number of tasks per job");
		cli_options.addOption("n", "numsamples", true, "number of samples to produce.  Multiply this by the sampling interval to get the number of jobs that will be run");
		cli_options.addOption("i", "samplinginterval", true, "samplig interval");
		cli_options.addOption("p", "savepath", true, "save some iterations of the simulation path (arrival time, service time etc...)");

		// I'm trying to re-use code here, but using OptionBuilder from commons-cli 1.2
		// in scala is problematic because it has these static methods and the have to
		// be called on the class in scala.
		OptionBuilder.isRequired();
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("the base name of the output files");
		OptionBuilder.withLongOpt("outfile");
		cli_options.addOption(OptionBuilder.create("o"));
		OptionBuilder.hasArgs();
		OptionBuilder.withDescription("queue type and arguments");
		OptionBuilder.withLongOpt("queuetype");
		cli_options.addOption(OptionBuilder.create("q"));
		OptionBuilder.withDescription("arrival process");
		OptionBuilder.withLongOpt("arrivalprocess");
		cli_options.addOption(OptionBuilder.create("A"));
		OptionBuilder.withDescription("service process");
		OptionBuilder.withLongOpt("serviceprocess");
		cli_options.addOption(OptionBuilder.create("S"));
		
		//CommandLineParser parser = new DefaultParser();
		val parser: CommandLineParser = new PosixParser();
		var options: CommandLine = null;
		try {
			options = parser.parse(cli_options, args);
		} catch {
		case e: ParseException => {
			val formatter: HelpFormatter = new HelpFormatter();
			formatter.printHelp("FJSimulator", cli_options);
			e.printStackTrace();
			System.exit(0);
		}
		}
		options
  }
  
  
  /**
   * Take the command line spec for a random intertime process and 
   * return a function that generates samples.
   * Maybe I am getting carried away with the scala functional stuff.
   * 
   * TODO: Weibull, Normal
   */
  def parseProcessArgs(args: Array[String]): () => Double = {
    if (args.length == 0) {
      () => 1.0
    } else {
    	args(0) match {

    	  // constant rate process
    	  case "c" if (args.length == 2) => {
    	    val rate = args(1).toDouble
    	    ( () => 1.0/rate )
    	  }
    	  
    	  // exponential/Poisson process
    	  case "x" if (args.length == 2) => {
    	    val rate = args(1).toDouble
    	    ( () => -Math.log(random)/rate )
    	  }
    	  
    	  // Erlang-k process
    	  case "e" if (args.length == 3) => {
    	    val k = args(1).toInt
    	    val rate = args(2).toDouble
    	    ( () => {
    	    	var p = 1.0
    	    	for ( i <- 1 to k) {
    	    		p *= random
    	    	}
    	    	-Math.log(p)/rate
    	    } )
    	  }    	    
    	}
    }
    () => 1.0
  }
  
  /**
   * Run s slices on the Spark cluster, with service times drawn
   * from the given serviceProcess.
   * 
   * Within each slice we just generate random numbers for the specified amount of time.
   * This is from the SparkPi demo program, generating random numbers in a square.
   * Each slice returns 1, and we do a count() to force Spark to execute the slices.
   * Therefore the shuffle/reduce step is trivial.
   * 
   */
  def runEmptySlices(spark:SparkContext, slices: Int, serviceProcess: () => Double): Long = {
    println("*** runEmptySlices( "+slices+" )")
    spark.parallelize(1 to slices, slices).map { i =>
      val jobLength = serviceProcess()
      val startTime = java.lang.System.currentTimeMillis()
      val targetStopTime = startTime + 1000*jobLength
			println("    +++ START: "+startTime)
			while (java.lang.System.currentTimeMillis() < targetStopTime) {
				val x = random * 2 - 1
				val y = random * 2 - 1
			}

      val stopTime = java.lang.System.currentTimeMillis()
      println("    --- STOP: "+stopTime)
      println("    === ELAPSED: "+(stopTime-startTime))
      1
    }.count()
  }
  
  
  /**
   * Main method
   * 
   * Sets up Spark environment ad runs the main loop of the program,
   * sending sliced jobs to the task manager.
   * 
   */
  def main(args: Array[String]) {
    
    val options: CommandLine = parseArgs(args)
    
    val arrivalProcess = parseProcessArgs(options.getOptionValues("A"))
    val serviceProcess = parseProcessArgs(options.getOptionValues("S"))
    
	  val conf = new SparkConf().setAppName("PoissonServiceArrivals")
	  println("*** got conf ***")
		val spark = new SparkContext(conf)
		println("*** got spark context ***")
		
		val totalSlices = if (args.length > 0) args(0).toInt else 2
		val slicesPerStep = if (args.length > 1) args(1).toInt else 1
		val rate = if (args.length > 2) args(2).toDouble else 0.2
		val serviceRate = if (args.length > 3) args(3).toDouble else 1.0
		var totalJobs = totalSlices/slicesPerStep
		if ((totalSlices%slicesPerStep) > 0) {
		  totalJobs += 1
		}
		println("*** totalSlices = "+totalSlices+" ***")
		println("*** slicesPerStep = "+slicesPerStep+" ***")
  	println("*** totalJobs = "+totalJobs+" ***")
		var slicesRun = 0
		
		// give the system a little time for the executors to start... 30 seconds?
		// this is stupid b/c the full set of executors actually take less tha 1s to start
		print("Waiting a moment to let the executors start...")
		Thread sleep 1000*30
		print("done waiting!")
		
		var doneSignal: CountDownLatch = new CountDownLatch(totalJobs)
		val initialTime = java.lang.System.currentTimeMillis()
		while (slicesRun < totalSlices) {
			println("")
			var s = math.min(slicesPerStep, (totalSlices - slicesRun));
			val t = new Thread(new Runnable {
				def run() {
					val startTime = java.lang.System.currentTimeMillis();
					println("+++ START: "+startTime)
					runEmptySlices(spark, s, serviceProcess)
					val stopTime = java.lang.System.currentTimeMillis()
					println("--- STOP: "+stopTime)
					println("=== ELAPSED: "+(stopTime-startTime))
					println("=== TOTAL ELAPSED: "+(1.0*(stopTime-initialTime)/1000.0))
					doneSignal.countDown()
				}
			});
			slicesRun += s;
			t.start()

			val interarrivalTime = arrivalProcess()
			println("*** inter-arrival time: "+interarrivalTime+" ***")
			Thread sleep math.round(interarrivalTime * 1000)

		}
		println("*** FINISHED!! ***")
		doneSignal.await()
		spark.stop()
  }
  
}

