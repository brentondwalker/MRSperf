#
# python scripts/process_spark_event_log.py -f workdir/app-20160503103936-0006 -o data/app-20160503103936-0006_t1_r15_s10 -d data/app-20160503103936-0006_t1_r15_s10.dist -b 10
#
# python scripts/process_spark_event_log.py -f fjpaper-data/app-20160802152503-0000_t1_e1_c1_r07_s10.gz -o fjpaper-data/app-20160802152503-0000_t1_e1_c1_r07_s10.dat -d fjpaper-data/app-20160802152503-0000_t1_e1_c1_r07_s10.dist -j fjpaper-data/app-20160802152503-0000_t1_e1_c1_r07_s10.jobdat -b 10
#
#

from pprint import pprint
import numpy
import argparse
import json
import gzip


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="eventlog file to parse",
                        dest="file", required=True)
    parser.add_argument("-o", "--outfile", help="output file for data",
                        dest="outfile", required=True)
    parser.add_argument("-d", "--distfile", help="output file for distributions",
                        dest="distfile", required=False)
    parser.add_argument("-j", "--jobdatafile", help="output file for raw job sojourn times etc",
                        dest="jobdatafile", required=False)
    parser.add_argument("-b", "--binwidth", help="width of bins used to compute distributions (in ms)",
                        dest="binwidth", type=int, default=1)
    args = parser.parse_args()
    
    # dict of events, indexed by job ID
    events = {}
    stage_to_job_lookup = {}
    
    with gzip.open(args.file, 'r') as f:
        for line in f:
            evt = json.loads(line)
            #pprint(evt)
            if evt['Event'] == 'SparkListenerJobStart':
                job_id = evt["Job ID"]
                stage_infos = evt["Stage Infos"]
                #print("SparkListenerJobStart "+str(job_id))
                events[job_id] = {
                                  'job_id': job_id,
                                  'submission_time': int(evt['Submission Time']),
                                  'stages': {}
                                  }
                for stage in stage_infos:
                    stage_id = stage['Stage ID']
                    num_tasks = stage['Number of Tasks']
                    stage_to_job_lookup[stage_id] = job_id
                    events[job_id]['stages'][stage_id] = {
                                                          'stage_id': stage_id,
                                                          'num_tasks': num_tasks,
                                                          'tasks': {}
                                                          }
            elif evt['Event'] == 'SparkListenerStageSubmitted':
                #print("SparkListenerStageSubmitted")
                pass
            elif evt['Event'] == 'SparkListenerTaskStart':
                stage_id = evt['Stage ID']
                job_id = stage_to_job_lookup[stage_id]
                task_info = evt['Task Info']
                task_id = task_info['Task ID']
                #print("SparkListenerTaskStart "+str(job_id)+" : "+str(stage_id)+" : "+str(task_id))
                stage = events[job_id]['stages'][stage_id]
                stage['tasks'][task_id] = {
                                         'task_id': task_id,
                                         'launch_time': task_info['Launch Time']
                                        }
            elif evt['Event'] == 'SparkListenerTaskEnd':
                if 'Task Metrics' not in evt:
                    print("WARNING: got SparkListenerTaskEnd event with no 'Task Metrics'")
                    continue
                stage_id = evt['Stage ID']
                job_id = stage_to_job_lookup[stage_id]
                task_info = evt['Task Info']
                task_metrics = evt['Task Metrics']
                task_id = task_info['Task ID']
                #print("SparkListenerTaskEnd "+str(job_id)+" : "+str(stage_id)+" : "+str(task_id))
                task = events[job_id]['stages'][stage_id]['tasks'][task_id]
                task['finish_time'] = task_info['Finish Time']
                task['ending_launch_time'] = task_info['Launch Time']
                task['deserialize_time'] = task_metrics['Executor Deserialize Time']
                task['run_time'] = task_metrics['Executor Run Time']
                task['serialization_time'] = task_metrics['Result Serialization Time']
            elif evt['Event'] == 'SparkListenerStageCompleted':
                stage_info = evt['Stage Info']
                stage_id = stage_info['Stage ID']
                job_id = stage_to_job_lookup[stage_id]
                #print("SparkListenerStageCompleted "+str(job_id)+" : "+str(stage_id))
                stage = events[job_id]['stages'][stage_id]
                stage['completion_time'] = stage_info['Completion Time']
                stage['submission_time'] = stage_info['Submission Time']
            elif evt['Event'] == 'SparkListenerJobEnd':
                job_id = evt['Job ID']
                #print("SparkListenerJobEnd "+str(job_id))
                events[job_id]['completion_time'] = evt['Completion Time']
    
    #pprint(events)
    
    # save some stats in tabular form
    max_time_interval = 0
    mean_waiting_sum = 0.0
    mean_sojourn_sum = 0.0
    mean_service_sum = 0.0
    mean_n = 0
    with open(args.outfile, 'w') as f, open(args.jobdatafile, 'w') as fj:
        for job_id in sorted(events.iterkeys()):
            # if we are processing a log that got truncated or is unfinished
            if 'completion_time' not in events[job_id]:
                continue
            #print("job_id: "+str(job_id))
            job = events[job_id]
            job_sub_time = job['submission_time']
            job_completion_time = job['completion_time']
            job['sojourn_time'] = job_completion_time - job_sub_time
            max_time_interval = max(max_time_interval, job['sojourn_time'])
            first_task_launch_time = 0
            for stage_id in sorted(job['stages'].iterkeys()):
                #print("stage_id: "+str(stage_id))
                stage = events[job_id]['stages'][stage_id]
                stage_submission_time = stage['submission_time']
                stage_completion_time = stage['completion_time']
                stage['sojourn_time'] = stage_completion_time - stage_submission_time
                for task_id in sorted(stage['tasks'].iterkeys()):
                    #print("task_id: "+str(task_id))
                    task = stage['tasks'][task_id]
                    # check if this task attempt finished?
                    if 'finish_time' not in task:
                        print("WARNING: found task with no finish time")
                        del stage['tasks'][task_id]
                        continue
                    launch_time = task['launch_time']
                    finish_time = task['finish_time']
                    run_time = task['run_time']
                    serialization_time = task['serialization_time']
                    f.write("\t".join([str(job_id), str(stage_id), str(task_id), \
                                    str(job_sub_time), str(job_completion_time), \
                                    str(stage_submission_time), str(stage_completion_time), \
                                    str(launch_time), str(finish_time), str(run_time), str(serialization_time)])
                            +"\n")
                    if (first_task_launch_time == 0) or (first_task_launch_time > launch_time):
                        first_task_launch_time = launch_time
            job['waiting_time'] = first_task_launch_time - job_sub_time
            mean_waiting_sum += job['waiting_time']
            mean_sojourn_sum += job['sojourn_time']
            mean_service_sum += job['sojourn_time'] - job['waiting_time']
            fj.write("\t".join([str(job_id), str(job['sojourn_time']), str(job['waiting_time']), str(job['sojourn_time'] - job['waiting_time'])])+"\n")
            mean_n += 1

    print("mean waiting time: "+str(mean_waiting_sum/mean_n)+"   (n="+str(mean_n)+")")
    print("mean sojourn time: "+str(mean_sojourn_sum/mean_n)+"   (n="+str(mean_n)+")")
    print("mean service time: "+str(mean_service_sum/mean_n)+"   (n="+str(mean_n)+")")
    
    if args.distfile:
        bin_width = args.binwidth
        distributions = {}
        for i in xrange(0, (max_time_interval/bin_width)+1):
            distributions[i] = {
                                 'dt': 1.0*i*bin_width/1000.0,
                                 'sojourn': 0,
                                 'waiting': 0,
                                 'service': 0
                                }
        for job_id in sorted(events.iterkeys()):
            # if we are processing a log that got truncated or is unfinished
            if 'completion_time' not in events[job_id]:
                continue
            job = events[job_id]
            distributions[job['sojourn_time']/bin_width]['sojourn'] += 1
            distributions[job['waiting_time']/bin_width]['waiting'] += 1
            service_time = job['sojourn_time'] - job['waiting_time']
            distributions[service_time/bin_width]['service'] += 1
                
        with open(args.distfile, 'w') as f:
            total = 1.0*len(events)
            sojourn_sum = 0.0
            waiting_sum = 0.0
            service_sum = 0.0
            for i in xrange(0, (max_time_interval/bin_width)+1):
                sojourn_sum += distributions[i]['sojourn']
                waiting_sum += distributions[i]['waiting']
                service_sum += distributions[i]['service']
                f.write("\t".join([str(distributions[i]['dt']),
                                   str(distributions[i]['sojourn']/total),
                                   str(distributions[i]['waiting']/total),
                                   str(distributions[i]['service']/total),
                                   str(sojourn_sum/total),
                                   str(waiting_sum/total),
                                   str(service_sum/total)])
                        +"\n")


if __name__ == "__main__":
    main()




