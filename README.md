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


## Running Spark

### Spark Docker Containers

To better conform to the queueing-theoretic models, this package is intended to be run with a large number of workers with one core each.  Of course most servers have lots of cores.  We can still run many independent single-core workers on each node by using [docker containers](https://www.docker.com/).  This set-up is based on containers used for Spark unit and integration testing.  They cite the following:
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

There is only one master, and it can be run outside a container.  We include a Spark config file to use in `spark-configs/spark-defaults.conf`.  The configuration enables event logs (that will be the data recorded in our experiments) and puts them in /mnt/event-logs/.  You may want to change that.  The files can get very large.
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
 



