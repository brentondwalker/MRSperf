from pprint import pprint
import numpy
import argparse
import json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="eventlog file to parse",
                        dest="file", required=True)
    parser.add_argument("-o", "--outfile", help="output file for data",
                        dest="outfile", required=True)
    args = parser.parse_args()
    
    # dict of events, indexed by job ID
    events = {}
    stage_to_job_lookup = {}
    
    with open(args.file, 'r') as f:
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
    with open(args.outfile, 'w') as f:
        for job_id in sorted(events.iterkeys()):
            #print("job_id: "+str(job_id))
            job = events[job_id]
            job_sub_time = job['submission_time']
            job_completion_time = job['completion_time']
            for stage_id in sorted(job['stages'].iterkeys()):
                #print("stage_id: "+str(stage_id))
                stage = events[job_id]['stages'][stage_id]
                stage_submission_time = stage['submission_time']
                stage_completion_time = stage['completion_time']
                for task_id in sorted(stage['tasks'].iterkeys()):
                    #print("task_id: "+str(task_id))
                    task = stage['tasks'][task_id]
                    launch_time = task['launch_time']
                    finish_time = task['finish_time']
                    run_time = task['run_time']
                    serialization_time = task['serialization_time']
                    f.write("\t".join([str(job_id), str(stage_id), str(task_id), \
                                    str(job_sub_time), str(job_completion_time), \
                                    str(stage_submission_time), str(stage_completion_time), \
                                    str(launch_time), str(finish_time), str(run_time), str(serialization_time)])
                            +"\n")
    
    
if __name__ == "__main__":
    main()




