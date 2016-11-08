package sparkarrivals;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.concurrent.CountDownLatch
import org.apache.spark.scheduler
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler._
import org.apache.spark.rdd.RDD

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.MissingOptionException;

import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.distribution.WeibullDistribution;

import scala.math.random
import scala.collection.mutable.ListBuffer
import java.io._
import java.util.concurrent.atomic.AtomicInteger
import scala.math.Ordering
import java.util.concurrent.PriorityBlockingQueue


/**
 * This is like InPlaceMultistageMap, but when we don't force Spark to
 * separate the map into different stages, it will optimize them into
 * a single stage.  This sounds like an improvement.
 * - in terms of overhead
 * - eliminated syncronization constraint between stages
 * but depending on the distribution of service times it may not be better
 * - it ties each chain of tasks to a single worker for its duration
 * 
 * We expect that if the stages have exponential service times, then each
 * pipelined multi-stage task should have Erlang-k service times.  But
 * sine this is for the sake of realism, and because we like to be able to study
 * service times besides exponential, we will actually construct the multi-stage
 * map jobs and let spark optimize them.
 */
object InPlaceMultistagePipelineMap {
  
  
  /**
   * Parse command line arguments using commons-cli 1.2
   * We have to use 1.2 even though it clunky in scala, because it is used
   * in Spark.  If we try to use 1.3 or later the packages will conflilct.
   * 
   */
  def parseArgs(args: Array[String]): CommandLine = {
    val cli_options: Options = new Options();
		//cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("w", "numworkers", true, "number of workers/servers");
		cli_options.addOption("t", "numtasks", true, "number of tasks per job");
		cli_options.addOption("n", "numsamples", true, "number of jobs to run");
		cli_options.addOption("o", "outfile-base", true, "base name (with path) of the output files");
		cli_options.addOption("r", "rounds", true, "number of rounds of service each job needs");
		cli_options.addOption("x", "correlated-rounds", false, "the service times of the tasks are the same across rounds");
		cli_options.addOption("s", "sequential-jobs", false, "submit the jobs sequentially instead of from separate threads");
		cli_options.addOption("c", "constructive", false, "build the multi-stage jobs in a functionally constructive way (default method is recursive)");
		
		// I'm trying to re-use code here, but using OptionBuilder from commons-cli 1.2
		// in scala is problematic because it has these static methods and the have to
		// be called on the class in scala.
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
		
		val parser: CommandLineParser = new PosixParser();
		var options: CommandLine = null;
		try {
			options = parser.parse(cli_options, args);
		} catch {
		case e: MissingOptionException => {
			System.err.println("\nERROR: Missing a required option!\n")
			val formatter: HelpFormatter = new HelpFormatter();
			formatter.printHelp(this.getClass.getSimpleName, cli_options);
			//e.printStackTrace();
			System.exit(0);
		}
		case e: ParseException => {
			System.err.println("ERROR: ParseException")
			val formatter: HelpFormatter = new HelpFormatter();
			formatter.printHelp(this.getClass.getSimpleName, cli_options);
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
   * TODO: Normal?
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
    	  
    	  // Weibull
    	  case "w" if (args.length == 2 || args.length == 3) => {
    	    val shape = args(1).toDouble
    	    val rate = if (args.length == 3) args(2).toDouble else 1.0
    	    val scale = 1.0/(rate * Gamma.gamma(1.0 + 1.0/shape));

    	    val weibul = new WeibullDistribution(shape, scale)
    	    ( () => weibul.sample() )
    	  }
    	  
    	  // otherwise
    	  case _ => {
    	    println("\n\nWARNING: unrecognized random process spec: \""+args.mkString(" ")+"\"  Using constant rate of 1.0\n\n")
    	    () => 1.0
    	  }
    	}
    }
  }
  
  
  // Data types for recording task information in an RDD that can be collected
  // back at the driver and saved.
  type TaskID = Tuple3[Int,Int,Int]
  type TaskData = Tuple3[TaskID,Long,Long]
  type JobData = Array[ListBuffer[TaskData]]
  
  // the jobs will return an Array[ListBuffer[TaskData]] and we need a way to
  // keep these things sorted by jobId
  val jobDataOrdering = new Ordering[JobData] {
    def compare(x:JobData,y:JobData): Int = {
      x(0).last._1._1 compare y(0).last._1._1
    }
  }
  
  /**
   * This is the code that the tasks execute.  It is just generating random numbers
   * for a desired length of time.  The desired execution time is passed in,
   * and this loop seems to hit it accurately down to the ms.
   */
  def doWork(jobLength: Double, jobId: Int, stageId: Int, taskId: Int): TaskData = {
	  val startTime = java.lang.System.currentTimeMillis();
	  val targetStopTime = startTime + 1000*jobLength;
	  println("---------\n    +++ TASK "+jobId+"."+stageId+"."+taskId+" START: "+startTime)
	  while (java.lang.System.currentTimeMillis() < targetStopTime) {
		  val x = random * 2 - 1;
		  val y = random * 2 - 1;
	  }

	  val stopTime = java.lang.System.currentTimeMillis();
	  println("    --- TASK "+jobId+"."+stageId+"."+taskId+" STOP: "+stopTime)
	  println("    === TASK "+jobId+"."+stageId+"."+taskId+" ELAPSED: "+(stopTime-startTime))
	  ((jobId, stageId, taskId), startTime, stopTime)
  }
  
  
  /**
   * Run s slices on the Spark cluster, with service times drawn
   * from the given serviceProcess.
   * 
   * Within each slice we just generate random numbers for the specified amount of time.
   * This is from the SparkPi demo program, generating random numbers in a square.
   * Each slice returns the task ID, and we do a count() to force Spark to execute the slices.
   * Therefore the shuffle/reduce step is trivial.
   * 
   * Note: the stdout produced from these println() will appear on the stdout of the workers,
   *       not the driver.  I could just as well remove it.
   *       
   *  Note: I would like to pass in the serviceProcess instead of a list of serviceTimes, but since
   *        this is parallelized I ran into the problem that in some cases we would be passing
   *        identical RNGs to the workers, and generating identical service times.
   */
  def runEmptySlicesPipeline(spark:SparkContext, slices: Int, serviceTimes: List[List[Double]], jobId: Int): Long = {
    //println("serviceTimes = "+serviceTimes)
    spark.parallelize(1 to slices, slices)
    .map { i =>
      val taskId = i
      val stageId = 0
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 1
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 2
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 3
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 4
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 5
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 6
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 7
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }
    .map { i =>
      val taskId = i
      val stageId = 8
      val jobLength = serviceTimes(stageId)(taskId-1)
      doWork(jobLength, jobId, stageId, taskId)._1._3
    }.count()
  }
  
  
  /**
   * Recursive multi-stage builder function.
   * 
   * This takes an RDD[(Int, Iterable[Int])], which is the form that is returned by a groupBy.
   * It does busywork in each slice for the specified amount of time.  Then each slice returns its
   * taskId, and it does a groupBy on this taskId (each slice will remain separate since they all
   * have distinct taskId).  Finally it recursively calls itself on the result.
   * 
   * The recursion terminates when remaining_rounds==0.
   * 
   * When running this for a certain number of rounds, Spark will appear to be executing two 
   * more rounds than was requested.  One of the extra rounds is the initial parallelize().groupBy()
   * that is passed in from runInPlaceRounds().  Typically that will be very short, about 20ms.
   * The other extra stage is the final count at the end.  That should also be fast, one the same
   * order as the initial stage.  Both of these tend to be anomalous in the first job, but stabilize
   * after.  Both extra jobs have very small executor time and are mainly overhead processing.
   * 
   */
  def recursiveInPlaceEmptyRoundsPipeline(rdd: RDD[ListBuffer[TaskData]], serviceTimes: List[List[Double]], jobId: Int, remaining_rounds: Int): RDD[ListBuffer[TaskData]] = {
    if (remaining_rounds == 0) {
      rdd
    } else {
    	recursiveInPlaceEmptyRoundsPipeline(
    			rdd.map { i =>
    			  val taskId = i.last._1._3
    			  val stageId = remaining_rounds
    			  val jobLength = serviceTimes.last(taskId-1)
    			  val prevData = i
    			  prevData.append(doWork(jobLength, jobId, stageId, taskId))
    			  prevData
    			},
    			if (serviceTimes.length==1) serviceTimes else serviceTimes.init,
    			jobId,
    			remaining_rounds - 1)
    }
  }
  
  /**
   * This is the entrypoint to the recursive recursiveInPlaceEmptyRounds.  It generates the random
   * service times, and then creates the initial RDD with the desired number of slices, and
   * does a groupBy so the RDD input is the correct type for the recursive function.
   * 
   * This method also calls count() on the end result of the recursion, which what triggers the
   * actual execution of the whole thing.
   */
  def runInPlaceRoundsPipelineRecursive(spark:SparkContext, numSlices: Int, serviceProcess: () => Double, numRounds: Int, jobId: Int, correlatedRounds: Boolean): Array[ListBuffer[TaskData]] = {

    // if we pass in just a single set of service times, ten the recursion
    // will use those same service times for each stage (correlated service times mode)
    val serviceTimes = 
      if (correlatedRounds) List(List.tabulate(numSlices)( n => serviceProcess() ))
      else List.tabulate(numRounds)(n => List.tabulate(numSlices)( n => serviceProcess() ) )
    
    recursiveInPlaceEmptyRoundsPipeline(
        spark.parallelize(1 to numSlices, numSlices)
          .map( x => ListBuffer[TaskData]( ((jobId,-1,x),0,0)) ),
        serviceTimes,
        jobId,
        numRounds)
    .collect
  }
  
  
  /**
   * Takes an n-stage function mapping (RDD[(Int, Iterable[Int])] => RDD[(Int, Iterable[Int])]),
   * and returns an (n+1)-stage function.
   */
  def appendEmptyStagePipeline(f: (RDD[ListBuffer[TaskData]] => RDD[ListBuffer[TaskData]]), serviceTimes: List[Double], jobId: Int, stageId: Int): RDD[ListBuffer[TaskData]] => RDD[ListBuffer[TaskData]] = {
    rdd: RDD[ListBuffer[TaskData]] => f(rdd).map { i =>
		  val taskId = i.last._1._3
      val jobLength = serviceTimes(taskId-1)
      val prevData = i
      prevData.append(doWork(jobLength, jobId, stageId, taskId))
      prevData
		}
  }
  
  
  /**
   * This is an alternate version of the program that iteratively builds a function
   * with the desired number of stages.  The result should behave the same as the recursive
   * version.
   */
  def runInPlaceRoundsPipelineConstructive(spark:SparkContext, slices: Int, serviceProcesss: () => Double, numRounds: Int, jobId: Int, correlatedRounds: Boolean): Array[ListBuffer[TaskData]] = {
    
    var f: RDD[ListBuffer[TaskData]] => RDD[ListBuffer[TaskData]] = { x => x }
    
    // if we are using correlated service times, then we will reuse these service times at every stage
    val taskServiceTimes = if (correlatedRounds) List.tabulate(slices)(n => serviceProcesss()) else null
    
    for ( stageId <- 1 to numRounds ) {
    	val serviceTimes = if (correlatedRounds) taskServiceTimes else List.tabulate(slices)(n => serviceProcesss())
      f = appendEmptyStagePipeline(f, serviceTimes, jobId, stageId)
    }
    
    f(spark.parallelize(1 to slices, slices)
        .map( x => ListBuffer[TaskData]( ((jobId,-1,x),0,0)) )
    )
    .collect
  }
  
  
  // the tasks are re-numbered from 0 to k in each job and stage.  In the output file
  // we want a global counter that we can plot against
  var globalTaskCounter: Int = 0

  /**
   * Take the result of running a multistage job and print out the data with the
   * sequence of events for each task on the same line.
   */
  def writeMultistageJobData(d:Array[ListBuffer[TaskData]], jobArrivalTime:Double, jobSubmissionTime:Long, jobDepartTime: Long, experimentStartTime:Long, ofile:BufferedWriter) {
    if (ofile != null) {
    	d.sortBy { x => x.last._1._3 }
    	.foreach { x => {
    		val fields:ListBuffer[Long] = ListBuffer[Long]();
    	  fields.append(globalTaskCounter)
    	  fields.append(x.last._1._1) // jobId
    	  fields.append(x.last._1._3) // taskId
    	  fields.append(jobArrivalTime.toLong)
    	  fields.append(jobSubmissionTime - experimentStartTime)
    	  fields.append(jobDepartTime - experimentStartTime)    	  
    	  x.sortBy { y => y._2 }
    	  .foreach { y => {
    		  if (y._1._2 > 0) {
    			  fields.append(y._1._2) // stageId
    			  fields.append(y._2 - experimentStartTime, y._3 - experimentStartTime)  // start/stop times
    		  }
    	  }
    	  }
    	  ofile.write(fields.mkString("\t")+"\n")
    	  globalTaskCounter += 1
    	}
    	}
    }
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
		val serialJobs = options.hasOption("s")
		val numRounds = if (options.hasOption("r")) options.getOptionValue("r").toInt else 1
		val constructFunction = options.hasOption("c")
		val correlatedRounds = options.hasOption("x")
		val outfileBase = if (options.hasOption("o")) options.getOptionValue("o") else "";
    
    // if we are logging data (besides spark's eventlog)
    var ofile: BufferedWriter = null;
		if (options.hasOption("o")) {
			val file = new File(outfileBase+".dat");
			ofile = new BufferedWriter(new FileWriter(file))
		}
    
	  val conf = new SparkConf()
	    .setAppName("InPlaceMultistagePipelineMap")
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
    
	  // we use this to assign jobIDs in separate threads, so use something thread-safe
		val jobIdCounter = new AtomicInteger(0)
		
		// we use this to track the number of jobs started outside the job-runner threads
		var jobsRun = 0
		
		// we want to print out the job data in order, so we use this to remember which
		// job should print out next
		var jobDepartIndex = 0
				
		// Our jobs may finish out of order.  We use this to hold jobs that
		// have departed before their predecessors so we can print out results in order
		val departedJobsBuffer = new PriorityBlockingQueue[JobData](100, jobDataOrdering)
		
		// how we wait for all jobs to finish before exiting
		// this may be unnecessary now that I keep a list of the threads and join() at the end
		// if nothing else this is a scala thing, and join() is using java
		val doneSignal: CountDownLatch = new CountDownLatch(totalJobs)
		
		val initialTime = java.lang.System.currentTimeMillis()
		var lastArrivalTime = 0L
		var totalInterarrivalTime = 0.0
		val jobArrivalTimes = new Array[Double](totalJobs)
		jobArrivalTimes(0) = 0.0
		val jobStartTimes = new Array[Long](totalJobs)
		val jobDepartTimes = new Array[Long](totalJobs)
		val threadList = ListBuffer[Thread]()
		val experimentStartTime: Long = java.lang.System.currentTimeMillis()
		
		while (jobsRun.get < totalJobs) {
			println("")
			
			val t = new Thread(new Runnable {
			  def run() {
				  val jobId = jobIdCounter.getAndIncrement
					val startTime = java.lang.System.currentTimeMillis();
				  jobStartTimes(jobId) = startTime;
				  lastArrivalTime = startTime;
					println("+++ JOB "+jobId+" START: "+startTime)
					
					// we would like to pass the serviceProcess in to this method and let the workers
					// generate their own service times, but in some cases they end up generating
					// from identical RNGs, and get the same result.  So we generate the service times here.
					// These are actually generated in different threads, but it's the same process, same JVM.
					//runEmptySlices(spark, slicesPerJob, List.tabulate(10)(n => List.tabulate(slicesPerJob)( n => serviceProcess())), jobId)

				  if (constructFunction) {
				    departedJobsBuffer.add(runInPlaceRoundsPipelineConstructive(spark, slicesPerJob, serviceProcess, numRounds, jobId, correlatedRounds))
				  } else {
				    departedJobsBuffer.add(runInPlaceRoundsPipelineRecursive(spark, slicesPerJob, serviceProcess, numRounds, jobId, correlatedRounds))
				  }
				  
					val stopTime = java.lang.System.currentTimeMillis()
					println("--- JOB "+jobId+" STOP: "+stopTime)
					println("=== JOB "+jobId+" ELAPSED: "+(stopTime-startTime))
					jobDepartTimes(jobId) = stopTime
					doneSignal.countDown()
				}
			});
			threadList += t;
			t.start();
			jobsRun += 1;

			// check if we can print out any job data
			if (! departedJobsBuffer.isEmpty) {
				if (departedJobsBuffer.peek()(0).last._1._1 == jobDepartIndex) {
					val jobId = departedJobsBuffer.peek()(0).last._1._1;
					writeMultistageJobData(
							departedJobsBuffer.poll(),
							jobArrivalTimes(jobId),
							jobStartTimes(jobId),
							jobDepartTimes(jobId),
							experimentStartTime,
							ofile)
					jobDepartIndex += 1
				}
			}

			// if requested, run jobs in split-merge style; job n can't start until job (n-1) is departed.
			if (serialJobs) {
			  t.join();
			}
			
			// compute the arrival time of the next job and sleep a bit if necessary
			if (jobsRun < totalJobs) {
				val curTime = java.lang.System.currentTimeMillis()
						val totalElapsedTime = (curTime- initialTime)/1000.0
						val interarrivalTime = arrivalProcess();
				//println("*** inter-arrival time: "+interarrivalTime+" ***")
				totalInterarrivalTime += interarrivalTime
				//println("totalInterarrivalTime = "+totalInterarrivalTime+"\t totalElapsedTime = "+totalElapsedTime)
				jobArrivalTimes(jobsRun) = totalInterarrivalTime * 1000.0

				if (totalElapsedTime < totalInterarrivalTime) {
					//println("sleep "+(math.round((totalInterarrivalTime - totalElapsedTime) * 1000.0)))
					Thread sleep math.round((totalInterarrivalTime - totalElapsedTime) * 1000.0)
				}
			}
			
		}

		println("*** waiting for jobs to finish... ***")
		doneSignal.await()
		for (t <- threadList) {
		  t.join()  // unnecessary?
		}
		
		// print out the data for any remaining jobs
		println("departedJobsBuffer.length = "+departedJobsBuffer.size())
		//for ( x <- departedJobsBuffer.iterator) {
			//val jobId = x(0).last._1._1;
		while (! departedJobsBuffer.isEmpty) {
		  val x = departedJobsBuffer.poll()
		  val jobId = x(0).last._1._1;
		  println("  writing data for jobId = "+jobId)
			writeMultistageJobData(
					x,
					jobArrivalTimes(jobId),
					jobStartTimes(jobId),
					jobDepartTimes(jobId),
					experimentStartTime,
					ofile)
			jobDepartIndex += 1
		}
		
		println("*** FINISHED!! ***")
		if (! spark.isStopped) {
			spark.stop()
		}
		
		if (ofile != null) {
		  ofile.close()
		}
  }
  
}

