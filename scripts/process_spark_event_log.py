#
# python scripts/process_spark_event_log.py -f fjpaper-data/app-20160802152503-0000_t1_e1_c1_r07_s10.gz -o fjpaper-data/app-20160802152503-0000_t1_e1_c1_r07_s10 -b 10
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
    parser.add_argument("-o", "--outfile", help="output file name base",
                        dest="outfile", required=True)
    parser.add_argument("-b", "--binwidth", help="width of bins used to compute distributions (in ms)",
                        dest="binwidth", type=int, default=1)
    parser.add_argument("-d", "--distfile", action='store_true', help="compute distribution of sojourn times etc",
                        default=True)
    args = parser.parse_args()
    
    # dict of events, indexed by job ID
    events = {}
    stage_to_job_lookup = {}
    total_jobs = 0
    total_tasks = 0
    executor_idletime = {}
    ex0_task_count = 0
    ex0_last_task_end = 0
    
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
                ex_id = task_info['Executor ID']
                #scheduler_delay = task_info['Launch Time'] - max(executor_idletime.get(ex_id,0), events[job_id]['submission_time'])
                #print(str(stage_id)+"\tdelay: "+str(scheduler_delay)+"\t"+str(task_info['Launch Time']-executor_idletime.get(ex_id,0))+"\t"+str(task_info['Launch Time']-events[job_id]['submission_time']))
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
                task['deserialization_time'] = task_metrics['Executor Deserialize Time']
                task['run_time'] = task_metrics['Executor Run Time']
                task['serialization_time'] = task_metrics['Result Serialization Time']
                ex_id = task_info['Executor ID']
                task['scheduler_delay'] = task['finish_time']-task['launch_time']-task['deserialization_time']-task['run_time']
                #print(str(stage_id)+"\tdelay: "+str(scheduler_delay)+"\t"+str(task_info['Launch Time']-executor_idletime.get(ex_id,0))+"\t"+str(task_info['Launch Time']-events[job_id]['submission_time'])+"\tdst: "+str(task['deserialization_time']))

                total_tasks += 1
                if ex_id == "1":
                    if ex0_last_task_end==0:
                        ex0_last_task_end = events[job_id]['submission_time']
                        executor_idletime[ex_id] = events[job_id]['submission_time']
                    #print("\t".join([str(ex0_task_count), str(task_id), str(stage_id), str(events[job_id]['submission_time']), str(task['launch_time']), str(task['ending_launch_time']), str(task['deserialization_time']), str(task['run_time']), str(task['finish_time']), str(ex0_last_task_end), str(executor_idletime.get(ex_id, 0)), str(task['finish_time']-task['launch_time']-task['deserialization_time']-task['run_time'])]))
                    ex0_task_count += 1
                    ex0_last_task_end = task['finish_time']
                
                executor_idletime[ex_id] = task['finish_time']
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
                total_jobs += 1
    
    #pprint(events)
    
    # save some stats in tabular form
    max_time_interval = 0
    mean_waiting_sum = 0.0
    mean_sojourn_sum = 0.0
    mean_service_sum = 0.0
    mean_deserialization_time_sum = 0.0
    mean_scheduler_delay_sum = 0.0
    mean_n = 0
    with open(args.outfile+".dat", 'w') as f, open(args.outfile+".jobdat", 'w') as fj:
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
                    deserialization_time = task['deserialization_time']
                    scheduler_delay = task['scheduler_delay']
                    
                    f.write("\t".join([str(job_id), str(stage_id), str(task_id), \
                                    str(job_sub_time), str(job_completion_time), \
                                    str(stage_submission_time), str(stage_completion_time), \
                                    str(launch_time), str(finish_time), str(run_time), \
                                    str(deserialization_time), str(scheduler_delay)])
                            +"\n")
                    if (first_task_launch_time == 0) or (first_task_launch_time > launch_time):
                        first_task_launch_time = launch_time
                    mean_deserialization_time_sum += deserialization_time
                    mean_scheduler_delay_sum += scheduler_delay

            job['waiting_time'] = first_task_launch_time - job_sub_time
            mean_waiting_sum += job['waiting_time']
            mean_sojourn_sum += job['sojourn_time']
            mean_service_sum += job['sojourn_time'] - job['waiting_time']
            fj.write("\t".join([str(job_id), str(job['sojourn_time']), str(job['waiting_time']), str(job['sojourn_time'] - job['waiting_time'])])+"\n")
            mean_n += 1

    print("mean waiting time: "+str(mean_waiting_sum/mean_n)+"   (n="+str(mean_n)+")")
    print("mean sojourn time: "+str(mean_sojourn_sum/mean_n)+"   (n="+str(mean_n)+")")
    print("mean service time: "+str(mean_service_sum/mean_n)+"   (n="+str(mean_n)+")")
    print("mean_deserialization_time: "+str(mean_deserialization_time_sum/total_tasks)+"   (n="+str(total_tasks)+")")
    print("mean_scheduler_delay: "+str(mean_scheduler_delay_sum/total_tasks)+"   (n="+str(total_tasks)+")")
    
    if args.distfile:
        bin_width = args.binwidth
        distributions = {}
        for i in xrange(0, (max_time_interval/bin_width)+1):
            distributions[i] = {
                                 'dt': 1.0*i*bin_width/1000.0,
                                 'sojourn': 0,
                                 'waiting': 0,
                                 'service': 0,
                                 'deserialization': 0,
                                 'scheduler': 0,
                                 'run_time': 0
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
            for stage_id in sorted(job['stages'].iterkeys()):
                stage = events[job_id]['stages'][stage_id]
                for task_id in sorted(stage['tasks'].iterkeys()):
                    task = stage['tasks'][task_id]
                    deserialization_time = task['deserialization_time']
                    scheduler_delay = task['scheduler_delay']
                    run_time = task['run_time']
                    distributions[deserialization_time]['deserialization'] += 1
                    #print("increment deserialization "+str(deserialization_time)+ " to "+str(distributions[deserialization_time]['deserialization']))
                    distributions[scheduler_delay]['scheduler'] += 1
                    #print("increment scheduler "+str(scheduler_delay)+ " to "+str(distributions[scheduler_delay]['scheduler']))
                    distributions[run_time/bin_width]['run_time'] += 1
                
        with open(args.outfile+".dist", 'w') as f:
            total = 1.0*len(events)
            sojourn_sum = 0.0
            waiting_sum = 0.0
            service_sum = 0.0
            deserialization_sum = 0.0
            scheduler_sum = 0.0
            run_time_sum = 0.0
            for i in xrange(0, (max_time_interval/bin_width)+1):
                sojourn_sum += distributions[i]['sojourn']
                waiting_sum += distributions[i]['waiting']
                service_sum += distributions[i]['service']
                deserialization_sum += distributions[i]['deserialization']
                scheduler_sum += distributions[i]['scheduler']
                run_time_sum += distributions[i]['run_time']
                f.write("\t".join([str(distributions[i]['dt']),
                                   str(distributions[i]['sojourn']/total),
                                   str(distributions[i]['waiting']/total),
                                   str(distributions[i]['service']/total),
                                   str((1.0*distributions[i]['deserialization'])/(1.0*total_tasks)),
                                   str((1.0*distributions[i]['scheduler'])/(1.0*total_tasks)),
                                   str(distributions[i]['run_time']/total_tasks),
                                   str(sojourn_sum/total),
                                   str(waiting_sum/total),
                                   str(service_sum/total),
                                   str(deserialization_sum/total_tasks),
                                   str(scheduler_sum/total_tasks),
                                   str(run_time_sum/total_tasks)])
                        +"\n")


if __name__ == "__main__":
    main()




