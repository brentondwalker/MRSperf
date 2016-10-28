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


object ThreadedMapJobs {
  
  
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
		//OptionBuilder.isRequired();
		//OptionBuilder.hasArg();
		//OptionBuilder.withDescription("the base name of the output files");
		//OptionBuilder.withLongOpt("outfile");
		//cli_options.addOption(OptionBuilder.create("o"));
		//OptionBuilder.hasArgs();
		//OptionBuilder.withDescription("queue type and arguments");
		//OptionBuilder.withLongOpt("queuetype");
		//cli_options.addOption(OptionBuilder.create("q"));
	
		OptionBuilder.isRequired();
		OptionBuilder.hasArgs();
		OptionBuilder.withDescription("arrival process");
		OptionBuilder.withLongOpt("arrivalprocess");
		cli_options.addOption(OptionBuilder.create("A"));
		OptionBuilder.isRequired();
		OptionBuilder.hasArgs();
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
   * Note: the stdout produced from these println() will appear on the stdout of the workers,
   *       not the driver.  I could just as well remove it.
   */
  def runEmptySlices(spark:SparkContext, slices: Int, serviceProcess: () => Double, jobId: Int): Long = {
    spark.parallelize(1 to slices, slices).map { i =>
      val taskId = i
      val jobLength = serviceProcess()
      val startTime = java.lang.System.currentTimeMillis()
      val targetStopTime = startTime + 1000*jobLength
			println("    +++ TASK "+jobId+"."+taskId+" START: "+startTime)
			while (java.lang.System.currentTimeMillis() < targetStopTime) {
				val x = random * 2 - 1
				val y = random * 2 - 1
			}

      val stopTime = java.lang.System.currentTimeMillis()
      println("    --- TASK "+jobId+"."+taskId+" STOP: "+stopTime)
      println("    === TASK "+jobId+"."+taskId+" ELAPSED: "+(stopTime-startTime))
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

	  val options: CommandLine = parseArgs(args);

    val arrivalProcess = parseProcessArgs(options.getOptionValues("A"))
    val serviceProcess = parseProcessArgs(options.getOptionValues("S"))

    val totalJobs = if (options.hasOption("n")) options.getOptionValue("n").toInt else 0
		val slicesPerJob = if (options.hasOption("t")) options.getOptionValue("t").toInt else 1
		val numWorkers = if (options.hasOption("w")) options.getOptionValue("w").toInt else 1
		
	  val conf = new SparkConf()
	    .setAppName("ThreadedMapJobs")
	    .set("spark.cores.max", numWorkers.toString())
		val spark = new SparkContext(conf)		
		
		// give the system a little time for the executors to start... 30 seconds?
		// this is stupid b/c the full set of executors actually take less tha 1s to start
		print("Waiting a moment to let the executors start...")
		Thread sleep 1000*10
    		
		// check how many cores were actually allocated
		// one core in the list will be the driver.  The rest should be workers
		// http://stackoverflow.com/questions/39162063/spark-how-many-executors-and-cores-are-allocated-to-my-spark-job
		val coresAllocated = spark.getExecutorMemoryStatus.map(_._1).toList.length - 1
		println("*** coresAllocated = "+coresAllocated+" ***")
    if (coresAllocated != numWorkers) {
      println("ERROR: could only allocate "+coresAllocated+" workers ("+numWorkers+" requested) - exiting.")
      spark.stop()
      System.exit(1)
    }
    
		var jobsRun = 0
		var doneSignal: CountDownLatch = new CountDownLatch(totalJobs)
		val initialTime = java.lang.System.currentTimeMillis()
		while (jobsRun < totalJobs) {
			println("")
			val t = new Thread(new Runnable {
				def run() {
				  val jobId = jobsRun
					val startTime = java.lang.System.currentTimeMillis();
					println("+++ JOB "+jobId+" START: "+startTime)
					runEmptySlices(spark, slicesPerJob, serviceProcess, jobId)
					val stopTime = java.lang.System.currentTimeMillis()
					println("--- JOB "+jobId+" STOP: "+stopTime)
					println("=== JOB "+jobId+" ELAPSED: "+(stopTime-startTime))
					//println("=== JOB "+jobId+" TOTAL ELAPSED: "+(1.0*(stopTime-initialTime)/1000.0))
					doneSignal.countDown()
				}
			});
			jobsRun += 1;
			t.start()
			
			val interarrivalTime = arrivalProcess()
			println("*** inter-arrival time: "+interarrivalTime+" ***")
			Thread sleep math.round(interarrivalTime * 1000)

		}
		doneSignal.await()
		println("*** FINISHED!! ***")
		spark.stop()
  }
  
}

