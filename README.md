Queueing-theoretic fork-join experiments with Spark.

Map-reduce systems like [Spark](http://spark.apache.org/) can be modeled as [fork-join queueing systems](https://arxiv.org/abs/1610.06309).  But on a live spark system the arrival and service times of jobs are much more complicated than what queueing theoretic models would assume.  The shuffle/reduce stages can be even more complex and depend on the specific setup of the Spark cluster.  This project provides some controllable Spark workloads designed to match what is assumed in fork-join queueing models.  This package is intended to validate queueing theoretic models of map-reduce systems, not to do performance benchmarking of clusters.

This package it set up to use Spark 1.6.x.  It may work on others, but we've never tried.

## Build

Requirements:
* [Spark 1.6.x](http://spark.apache.org/downloads.html)
* [sbt](http://www.scala-sbt.org/)
* [sbt-assembly plugin](https://github.com/sbt/sbt-assembly)

```
sbt assembly
```

The jar file will be put in `target/scala-2.10/spark-arrivals-assembly-1.0.jar`.


## Run

This package is intended to be run with a large number of workers with one core each.  See the next section on how to set that up.

To submit the job you need the name/ip of the master.  Here is an example that runs an experiment with exponential arrivals with rate 0.7 and exponential service times with rate 1.0.
```
export SPARKHOME=<your_spark_directory>
export SPARKARRIVALS=<your_spark-arrivals_directory>
export SPARK_MASTER="<ip_or_name_of_spark_master>"

cd $SPARKHOME
bin/spark-submit --master spark://$SPARK_MASTER:7077 --class sparkarrivals.ThreadedMapJobs $SPARKARRIVALS/target/scala-2.10/spark-arrivals-assembly-1.0.jar -n <num-jobs> -t <tasks_per_job> -w <num_workers> -A x 0.7 -S x 1.0
```

## Processing Results

The data from an experiment is stored in an event log, probably on the machine where the job is submitted from, or possibly on the machine running the Spark master depending on how you configure your system.  Make sure in your spark configuration.  The event log files will have names like `app-20161028095007-0003` and contain data in JSON format.

We include a python script, `process_spark_event_log.py` to extract the useful data from these JSON files.
```
usage: process_spark_event_log.py [-h] -f FILE -o OUTFILE [-b BINWIDTH] [-d]
                                  [-z]

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  eventlog file to parse
  -o OUTFILE, --outfile OUTFILE
                        output file name base
  -b BINWIDTH, --binwidth BINWIDTH
                        width of bins used to compute distributions (in ms)
  -d, --distfile        compute distribution of sojourn times etc
  -z, --infile-gzipped  input file is gzipped
```

For example:
```
python scripts/process_spark_event_log.py -f ~/eventlogs/app-20161028155421-0011 -o mydata -b 10 -d
mean waiting time: 17.8   (n=10)
mean sojourn time: 28689.2   (n=10)
mean service time: 28671.4   (n=10)
mean_deserialization_time: 7888.68   (n=50)
mean_scheduler_delay: 25.62   (n=50)
```

This produces two or three ouput files, each with a name prefixed with whatever is given with the `-o <filebase>` option.
* Job data file `<filebase>.jobdat` contains job_id, job_sojourn_time, job_waiting_time, (job_sojourn_time - job_waiting_time)
* Experiment path file `<filebase>.path` contains per-task timing information for the events related to each task.
* Distributions file `<filebase>.dist` (if requested with the `-d` option) contains histograms of the job sojourn, waiting, and service times, as well as task deserialization time and scheduler delay.  The deserialization time and scheduler delay are binned at the maximum 1ms resoloution reported by Spark.  The other statistics are binned with binwidth specified with the `-b` argument.

For example, if we want to plot the experiment path with gnuplot:
```
plot 'mydata.path' using 3:4 with line title "job arrival"
replot 'mydata.path' using 3:8 with line title "task start"
replot 'mydata.path' using 3:9 with line title "task completion"
replot 'mydata.path' using 3:5 with line title "job departure"
```

#### Note on waiting times
This script computes the job waiting time as the time from job arrival until he first task stars service, and the service time as the time from the first task starting service until the job departs.  This is different from a lot of FJ queueing theory convention where these quantities are computed relative to when the *last* task starts service.  This is just what made sense to me.  All the information is still there, and the computation is easy to change if desired.


## Running Spark

### Spark Docker Containers

To better conform to the queueing-theoretic models, this package is intended to be run with a Spark cluster in [stand-alone mode](http://spark.apache.org/docs/1.6.2/spark-standalone.html) with a large number of workers with one core each.  Of course most servers have lots of cores.  We can still run many independent single-core workers on each node by using [docker containers](https://www.docker.com/).  This set-up is based on containers used for Spark unit and integration testing.  They cite the following:
```
Drawn from Matt Massie's docker files (https://github.com/massie/dockerfiles),
as well as some updates from Andre Schumacher (https://github.com/AndreSchumacher/docker).
```

The worker containers are configured to use 4GB of RAM each.  You may want to change that by editing `docker/spark-test/build`.  For the `ThreadedMapJobs` program, very little RAM is actually needed on the workers.

To build the containers, do the following on each node that will run Spark workers:
```
cd docker
./build
```

### Start Spark Master

There is only one master, and it can be run outside a container.  We include a Spark config file to use in `spark-configs/spark-master-defaults.conf`.  The configuration enables event logs (that will be the data recorded in our experiments) and puts them in /mnt/event-logs/ on the server running Spark master.  You may want to change that.  The files can get very large.
```
export SPARKHOME=<your_spark_directory>
export SPARKARRIVALS=<your_spark-arrivals_directory>
export SPARK_LOCAL_IP=<ip_address_to use>

cd $SPARKHOME
sbin/start-master.sh --properties-file $SPARKARRIVALS/spark-configs/spark-master-defaults.conf -h $SPARK_LOCAL_IP
```

### Start Spark Workers

To start the workers you need to have a separate IP address configured for each container.  If you want to run 10 workers, the the node needs at least 10 IP addresses.  They can be configured on the same interface.  We include a script, `start-workers.sh` that starts the worker containers.  It assumes that you are using IP addresses in the private 127.23.27.x range, and that the IP addresses used on each node are consecutive.

For example suppose we have a server with IPs 127.23.27.100 - 127.23.27.109.  Then we run the following:

```
export SPARKHOME=<your_spark_directory>
export SPARKARRIVALS=<your_spark-arrivals_directory>
export SPARK_MASTER="<ip_or_name_of_spark_master>"

cd $SPARKHOME
$SPARKARRIVALS/docker/start-workers.sh 100 10 $SPARK_MASTER
```

Then you can see the containers with `docker ps -a`.  You should also see the 1-core workers listed in the spark master's web interface.  You can kill all your docker containers (be careful!!) with `docker rm -f $(docker ps -a -q)`.


# Programs

Our intention is that this project will contain programs producing many types of workloads of varying complexity.

## `ThreadedMapJobs`

This program submits jobs with arrival times governed by a random arrival process, containing tasks with service times govered by a random service process.  The number of tasks per job is also contollable, but constant throughout a run of the prgram.  

The jobs only have a single map stage.  The shuffle/reduce is a trivial `count()` of the completed tasks.

The jobs are submitted from separate threads.  This means that a job can be queued before the previous job completes, and if one job has a particularly time-consuming task, jobs can overtake each other and finish out-of-order.  This is a characteristic (and a feature) of non-idling single-queue fork-join systems.

If the jobs were submitted from a single thread, then each job could not start processing until all tasks of the previous thread completed.  Then system would behave like a split-merge system, which generally has much worse performance.  In such a system the workers spend time idling when there are jobs waiting to process.




