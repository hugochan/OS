#!/usr/bin/python

import time
from copy import deepcopy
from collections import deque, OrderedDict
from indexed_priority_queue import IndexMinPQ
from indexed_pq import IndexedPQ

class BaseOS(object):
    """docstring for BaseOS"""
    def __init__(self):
        super(BaseOS, self).__init__()
        self.process_table = OrderedDict() # process table
        self.t_cs = 13 # context switch cost
        self.avg_wait_time = 0.0
        self.avg_turnaround_time = 0.0
        self.switch_count = 0
        self.burst_count = 0

    def load_process(self, filename):
        try:
            with open(filename, 'r') as f:
                for each_line in f:
                    if each_line == '' or each_line[0] == '#':
                        continue
                    conf = each_line.rstrip('\n').split('|') # <proc-num>|<burst-time>|<num-burst>|<io-time>
                    # process table {'proc-num':['burst-time', 'num-burst', 'io-time', 'status', 'eachstage-start-time']}
                    self.process_table[int(conf[0])] = dict(zip(['burst_time', 'num_burst', 'io_time', 'priority', 'status', 'start_time', 'next_burst_time'], [int(x) for x in conf[1:]] + [0, 0, int(conf[1])]))
        except Exception, e:
            print e
            assert False
        f.close()

    def run_proc(self, algo):
        # copy process table
        process_table = deepcopy(self.process_table) # copy both th parent and child object
        if algo == 'FCFS':
            self.run_proc_fcfs(process_table)
        elif algo == 'SRT':
            self.run_proc_srt(process_table)
        elif algo == 'PWA':
            self.run_proc_pwa(process_table)
        else:
            raise ValueError('invalid arg: %s'%algo)

    def run_proc_fcfs(self, process_table): # handle process queue
        # stats
        self.switch_count = 0
        avg_burst_time = 0.0
        self.avg_wait_time = 0.0
        # add the process queue
        process_queue = deque();
        for proc_num in process_table.keys():
            process_queue.append(proc_num)
        # All "ties" are to be broken using process number order
        process_table = OrderedDict(sorted(process_table.iteritems(), key=lambda d:d[0]))

        print "time 0ms: Simulator started for FCFS [Q " + ("%s"%list(process_queue))[1:]
        self.t0 = time.time()
        while len(process_table):
            if len(process_queue) == 0: # the CPU is idle
                self.poll_io_fcfs(process_table, process_queue)
            else:
                # context switch: the process of storing and restoring the state (more specifically, the execution context) of a process
                # poll io performance
                current_process = process_queue.popleft()
                self.avg_wait_time += (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
                t1 = time.time()
                while int(1000*(time.time() - t1)) < self.t_cs:
                    self.poll_io_fcfs(process_table, process_queue)
                self.switch_count += 1
                # switch to the next process
                process_table[current_process]['status'] = 1 # actively using the CPU
                process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                t = time.time()
                print "time %sms: P%s started using the CPU [Q "%(int(1000*(t-self.t0)), current_process) + ("%s"%list(process_queue))[1:]
                while int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'] < process_table[current_process]['burst_time']:
                    self.poll_io_fcfs(process_table, process_queue) # poll io performance
                # handle CPU
                if process_table[current_process]['num_burst'] == 1: # it is the last CPU burst
                    del process_table[current_process] # delete the completed process
                    print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue))[1:]
                else:
                    if process_table[current_process]['io_time'] > 0:
                        print "time %sms: P%s completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue))[1:]
                        print "time %sms: P%s performing I/O [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue))[1:]
                        process_table[current_process]['status'] = 2 # blocked on (or performing) I/O
                        process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                    else:
                        process_table[current_process]['status'] = 0 # ready to use the CPU
                        process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                        process_table[current_process]['num_burst'] -= 1
                        print "time %sms: P%s completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue))[1:]
                        process_queue.append(current_process)
        print "time %sms: Simulator for FCFS ended"%(int(1000*(time.time()-self.t0)))
        # stat
        burst_num = 0
        for val in self.process_table.values():
            avg_burst_time += (val['burst_time']*val['num_burst'])
            burst_num += val['num_burst']
        avg_burst_time /= burst_num
        self.avg_wait_time /= burst_num
        print "Algorithm FCFS"
        print "-- average CPU burst time: %.2f ms"%avg_burst_time
        print "-- average wait time: %.2f ms"%self.avg_wait_time
        print "-- average turnaround time: %.2f ms"%(self.avg_wait_time + avg_burst_time + self.t_cs)
        print "-- total number of context switches: %s"%self.switch_count

    def run_proc_srt(self, process_table):
        self.avg_wait_time = 0.0
        self.switch_count = 0
        avg_burst_time = 0.0
        # add the process queue
        process_queue = IndexMinPQ();
        for proc_num, values in process_table.iteritems():
            process_queue.insert(proc_num, values['burst_time']) # insert proc_num: cpu burst time
        # All "ties" are to be broken using process number order
        process_table = OrderedDict(sorted(process_table.iteritems(), key=lambda d:d[0]))

        print "time 0ms: Simulator started for SRT [Q " + ("%s"%list(process_queue.keys()))[1:]
        self.t0 = time.time()
        while len(process_table):
            self.proc_srt_loop(process_table, process_queue)
        print "time %sms: Simulator for SRT ended"%(int(1000*(time.time()-self.t0)))
        # stat
        burst_num = 0
        for val in self.process_table.values():
            avg_burst_time += (val['burst_time']*val['num_burst'])
            burst_num += val['num_burst']
        self.avg_turnaround_time = (self.avg_wait_time + self.t_cs*self.switch_count + avg_burst_time)/burst_num
        avg_burst_time /= burst_num
        self.avg_wait_time /= burst_num
        print "Algorithm SRT"
        print "-- average CPU burst time: %.2f ms"%avg_burst_time
        print "-- average wait time: %.2f ms"%self.avg_wait_time
        print "-- average turnaround time: %.2f ms"%self.avg_turnaround_time
        print "-- total number of context switches: %s"%self.switch_count

    def run_proc_pwa(self, process_table): # handle process queue
        self.avg_wait_time = 0.0
        self.switch_count = 0
        avg_burst_time = 0.0
        self.avg_turnaround_time = 0.0
        self.starvation_count = dict(zip(process_table.keys(), [0 for x in process_table.keys()]))
        # add the process queue
        process_queue = IndexedPQ();
        for proc_num, values in process_table.iteritems():
            process_queue.insert(proc_num, values['priority'])
        # All "ties" are to be broken using process number order
        process_table = OrderedDict(sorted(process_table.iteritems(), key=lambda d:d[0]))
        print "time 0ms: Simulator started for PWA [Q " + ("%s"%list(process_queue.keys()))[1:]
        self.t0 = time.time()
        while len(process_table):
            self.proc_pwa_loop(process_table, process_queue)
        print "time %sms: Simulator for PWA ended"%(int(1000*(time.time()-self.t0)))
        # stat
        burst_num = 0
        for val in self.process_table.values():
            avg_burst_time += (val['burst_time']*val['num_burst'])
            burst_num += val['num_burst']
        for k, v in self.starvation_count.items():
            self.avg_wait_time += (v*(3*self.process_table[k]['burst_time']+1))
        self.avg_turnaround_time = (self.avg_wait_time + self.t_cs*self.switch_count + avg_burst_time)/burst_num
        avg_burst_time /= burst_num
        self.avg_wait_time /= burst_num
        print "Algorithm PWA"
        print "-- average CPU burst time: %.2f ms"%avg_burst_time
        print "-- average wait time: %.2f ms"%self.avg_wait_time
        print "-- average turnaround time: %.2f ms"%self.avg_turnaround_time
        print "-- total number of context switches: %s"%self.switch_count

    def proc_srt_loop(self, process_table, process_queue, current_process=None):
        if process_queue.isEmpty(): # the CPU is idle
            self.poll_io_srt(process_table, process_queue) # preemption makes no sense in this case
        else:
            # context switch: the process of storing and restoring the state (more specifically, the execution context) of a process
            # poll io performance
            if not current_process:
                current_process = process_queue.delMin()
                self.avg_wait_time += (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
            t1 = time.time()
            while int(1000*(time.time() - t1)) < self.t_cs:
                self.poll_io_srt(process_table, process_queue) # preemption makes no sense in this case ???
            self.switch_count += 1

            # switch to the next process
            process_table[current_process]['status'] = 1 # actively using the CPU
            process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
            print "time %sms: P%s started using the CPU [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]

            while int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'] < process_table[current_process]['next_burst_time']:
                ret_val = self.poll_io_srt(process_table, process_queue, current_process) # poll io performance, preemption may occur
                if not ret_val == -1: # a preemption has occurred
                    # context switch
                    # poll io performance
                    self.proc_srt_loop(process_table, process_queue, ret_val)
                    return
            # handle CPU
            if process_table[current_process]['num_burst'] == 1: # it is the last CPU burst
                del process_table[current_process] # delete the completed process
                print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]
            else:
                print "time %sms: P%s completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]
                process_table[current_process]['next_burst_time'] = process_table[current_process]['burst_time']
                if process_table[current_process]['io_time'] > 0:
                    print "time %sms: P%s performing I/O [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]
                    process_table[current_process]['status'] = 2 # blocked on (or performing) I/O
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                else:
                    process_table[current_process]['status'] = 0 # ready to use the CPU
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                    process_table[current_process]['num_burst'] -= 1
                    process_queue.insert(current_process, process_table[current_process]['next_burst_time'])

    def proc_pwa_loop(self, process_table, process_queue, current_process=None):
        if process_queue.isEmpty(): # the CPU is idle
            self.poll_io_pwa(process_table, process_queue, current_process) # preemption makes no sense in this case
        else:
            # context switch: the process of storing and restoring the state (more specifically, the execution context) of a process
            # poll io performance
            if not current_process:
                current_process = process_queue.delMin()
                # stat
                self.avg_wait_time += (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
            t1 = time.time()
            while int(1000*(time.time() - t1)) < self.t_cs:
                self.poll_io_pwa(process_table, process_queue, current_process) # preemption makes no sense in this case ???
            # switch to the next process
            print "time %sms: P%s started using the CPU [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]
            self.switch_count += 1
            process_table[current_process]['status'] = 1 # actively using the CPU
            process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
            while int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'] < process_table[current_process]['next_burst_time']:
                ret_proc = self.poll_io_pwa(process_table, process_queue, current_process) # poll io performance, preemption may occur
                if not ret_proc == -1: # a preemption has occurred
                    # context switch
                    # poll io performance
                    self.proc_pwa_loop(process_table, process_queue, ret_proc)
                    return
            # handle CPU
            if process_table[current_process]['num_burst'] == 1: # it is the last CPU burst
                del process_table[current_process] # delete the completed process
                print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]
            else:
                print "time %sms: P%s completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]
                process_table[current_process]['next_burst_time'] = process_table[current_process]['burst_time']
                if process_table[current_process]['io_time'] > 0:
                    print "time %sms: P%s performing I/O [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(process_queue.keys()))[1:]
                    process_table[current_process]['status'] = 2 # blocked on (or performing) I/O
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                else:
                    process_table[current_process]['status'] = 0 # ready to use the CPU
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                    process_table[current_process]['num_burst'] -= 1
                    process_queue.insert(current_process, process_table[current_process]['priority'])

    def poll_io_fcfs(self, process_table, process_queue):
        # check IO performance (two processes complete io at the same time?)
        for k, v in process_table.items():
            if v['status'] == 2: #blocked on (or performing) I/O
                if int(1000*(time.time()-self.t0)) - v['start_time'] >= v['io_time']:
                    process_table[k]['status'] = 0 # ready to use the CPU
                    process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                    process_table[k]['num_burst'] -= 1
                    if process_table[k]['num_burst'] > 0:
                        print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue))[1:]
                        process_queue.append(k)
                    else:
                        del process_table[k] # delete the completed process
                        print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue))[1:]

    def poll_io_srt(self, process_table, process_queue, current_process=None):
        # check IO performance (two processes complete io at the same time?)
        preemption_flag = False # preemption ties are broken using process number order
                                # preemption occurs at most once in an io poll
        preemption_process = -1
        for k, v in process_table.items():
            if v['status'] == 2: #blocked on (or performing) I/O
                if int(1000*(time.time()-self.t0)) - v['start_time'] >= v['io_time']:
                    process_table[k]['num_burst'] -= 1
                    if process_table[k]['num_burst'] > 0:
                        # preemptive
                        if not current_process or preemption_flag:
                            process_table[k]['status'] = 0 # ready to use the CPU
                            process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                            process_queue.insert(k, process_table[k]['next_burst_time'])
                            print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
                        else:
                            current_proc_remaining_time = process_table[current_process]['next_burst_time'] - \
                                    (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
                            if process_table[k]['next_burst_time'] < current_proc_remaining_time:
                                # a preemption occurs
                                print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
                                process_table[current_process]['status'] = 0 # ready to use the CPU
                                process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                                process_table[current_process]['next_burst_time'] = current_proc_remaining_time
                                process_queue.insert(current_process, current_proc_remaining_time) # run the remaining time next round
                                process_table[k]['status'] = 3 # to avoid being polled io again
                                preemption_flag = True
                                preemption_process = k
                                print "time %sms: P%s preempted by P%s [Q "%(int(1000*(time.time()-self.t0)), current_process, k) + ("%s"%list(process_queue.keys()))[1:]
                            else:
                                process_table[k]['status'] = 0 # ready to use the CPU
                                process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                                process_queue.insert(k, process_table[k]['next_burst_time'])
                                print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
                    else:
                        del process_table[k] # delete the completed process
                        print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
        return preemption_process

    def poll_io_pwa(self, process_table, process_queue, current_process):
        multiplier = 3
        preemption_flag = False # preemption ties are broken using process number order
                                # preemption occurs at most once in an io poll
        preemption_process = -1
        # check IO performance (two processes complete io at the same time?)
        for k, v in process_table.items():
            if v['status'] == 2: # blocked on (or performing) I/O
                if int(1000*(time.time()-self.t0)) - v['start_time'] >= v['io_time']:
                    process_table[k]['num_burst'] -= 1
                    if process_table[k]['num_burst'] > 0:
                        # preemptive
                        if not current_process or preemption_flag:
                            process_table[k]['status'] = 0 # ready to use the CPU
                            process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                            process_queue.insert(k, process_table[k]['priority'])
                            print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
                        else:
                            if process_table[k]['priority'] < process_table[current_process]['priority']:
                                # a preemption occurs
                                print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
                                current_proc_remaining_time = process_table[current_process]['next_burst_time'] - \
                                    (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
                                process_table[current_process]['next_burst_time'] = current_proc_remaining_time
                                process_table[current_process]['status'] = 0 # ready to use the CPU
                                process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                                process_queue.insert(current_process, process_table[current_process]['priority'])
                                process_table[k]['status'] = 3 # to avoid being polled io again
                                preemption_flag = True
                                preemption_process = k
                                print "time %sms: P%s preempted by P%s [Q "%(int(1000*(time.time()-self.t0)), current_process, k) + ("%s"%list(process_queue.keys()))[1:]
                            else:
                                process_table[k]['status'] = 0 # ready to use the CPU
                                process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                                process_queue.insert(k, process_table[k]['priority'])
                                print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
                    else:
                        del process_table[k] # delete the completed process
                        print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(process_queue.keys()))[1:]
            if v['status'] == 0:
                if int(1000*(time.time()-self.t0)) - v['start_time'] > multiplier*v['burst_time']:
                    process_table[k]['priority'] -= 1;
                    if process_table[k]['priority'] < 0:
                        process_table[k]['priority'] = 0
                    self.starvation_count[k] += 1
                    # preemptive
                    if not current_process or preemption_flag:
                        process_queue.change(k, process_table[k]['priority'])
                        process_table[k]['start_time'] = int(1000*(time.time()-self.t0)) # reset the start time for status 0
                    else:
                        if process_table[k]['priority'] < process_table[current_process]['priority']:
                            # preemption occurs
                            current_proc_remaining_time = process_table[current_process]['next_burst_time'] - \
                                    (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
                            process_table[current_process]['next_burst_time'] = current_proc_remaining_time
                            process_table[current_process]['status'] = 0 # ready to use the CPU
                            process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                            process_queue.insert(current_process, process_table[current_process]['priority'])
                            process_queue.delete(k)
                            process_table[k]['status'] = 3 # to avoid being polled io again
                            preemption_flag = True
                            preemption_process = k
                            print "time %sms: P%s preempted by P%s [Q "%(int(1000*(time.time()-self.t0)), current_process, k) + ("%s"%list(process_queue.keys()))[1:]
                        else:
                            process_queue.change(k, process_table[k]['priority'])
                            process_table[k]['start_time'] = int(1000*(time.time()-self.t0)) # reset the start time for status 0
        return preemption_process

if __name__ == '__main__':
    import sys
    bos = BaseOS()
    try:
        in_file = sys.argv[1]
    except:
        print "ERROR: please input the filename"
        exit()
    bos.load_process(in_file)
    # bos.load_process('processes.txt')
    bos.run_proc('FCFS')
    print '\n'
    bos.run_proc('SRT')
    print '\n'
    bos.run_proc('PWA')
