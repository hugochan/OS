#!/usr/bin/python

import time
from copy import deepcopy
from collections import deque, OrderedDict
from indexed_priority_queue import IndexMinPQ

class BaseOS(object):
    """
    Process status:
    (a) waiting to be admitted into the system (i.e., memory allocation); --> 3
    (b) ready to use the CPU; --> 0
    (c) actively using the CPU; --> 1
    (d) blocked on (or performing) I/O; and --> 2
    (e) exiting the system (i.e., memory deallocation). --> 4
    """
    def __init__(self):
        super(BaseOS, self).__init__()
        self.process_table = OrderedDict() # process table
        self.t_cs = 13 # context switch cost
        self.t_slice = 80 # time slice for Round Robin algorithm
        self.t_memmove = 10 # the time to move one unit of memory
        self.t_pseudo_elapsed = 0 # while defragmentation is running, all processes are essentially placed in a suspended state, using pseudo elapsed time to simulate it
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
                    conf = each_line.rstrip('\n').split('|') # <proc-num>|<arrival-time>|<burst-time>|<num-burst>|<io-time>|<memory>
                    # process table {'proc-num':['arrival-time', 'burst-time', 'num-burst', 'memory', 'io-time', 'status', 'eachstage-start-time']}
                    self.process_table[conf[0]] = dict(zip(['arrival_time', 'burst_time', 'num_burst', 'io_time', 'memory', 'status', 'start_time', 'next_burst_time'], [int(x) for x in conf[1:]] + [3, 0, int(conf[2])]))
        except Exception, e:
            print e
            assert False
        f.close()

    def run_proc(self, algo, placement_algo):
        # memory pool
        memory_pool = MemPool(32, 8) # 32 units per line, 8 lines, total 256 units
        self.t_pseudo_elapsed = 0 # reset
        # copy process table
        process_table = deepcopy(self.process_table) # copy both th parent and child object
        # All "ties" are to be broken using process number order
        process_table = OrderedDict(sorted(process_table.iteritems(), key=lambda d:d[0]))

        if algo == 'RR': # Round Robin
            self.run_proc_rr(process_table, memory_pool, placement_algo)
        elif algo == 'SRT': # Shortest Remaining time
            self.run_proc_srt(process_table, memory_pool, placement_algo)
        else:
            raise ValueError('invalid arg: %s'%algo)

    def run_proc_srt(self, process_table, memory_pool, placement_algo):
        self.avg_wait_time = 0.0
        self.switch_count = 0
        avg_burst_time = 0.0

        print "time 0ms: Simulator started for SRT and %s"%placement_algo
        self.t0 = time.time()

        # at the very begining when no process has arrived
        process_queue = IndexMinPQ();
        self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)

        while len(process_table):
            self.proc_srt_loop(process_table, process_queue, memory_pool, placement_algo)
        print "time %sms: Simulator for SRT and %s ended"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, placement_algo)
        # stat
        burst_num = 0
        for val in self.process_table.values():
            avg_burst_time += (val['burst_time']*val['num_burst'])
            burst_num += val['num_burst']
        self.avg_turnaround_time = (self.avg_wait_time + self.t_cs*self.switch_count + avg_burst_time + self.t_pseudo_elapsed)/burst_num
        avg_burst_time /= burst_num
        self.avg_wait_time /= burst_num
        print "Algorithm SRT %s"%placement_algo
        print "-- average CPU burst time: %.2f ms"%avg_burst_time
        print "-- average wait time: %.2f ms"%self.avg_wait_time
        print "-- average turnaround time: %.2f ms"%self.avg_turnaround_time
        print "-- total number of context switches: %s"%self.switch_count

    def run_proc_rr(self, process_table, memory_pool, placement_algo): # handle process queue
        # stats
        self.switch_count = 0
        avg_burst_time = 0.0
        self.avg_wait_time = 0.0

        print "time 0ms: Simulator started for RR and %s"%placement_algo
        self.t0 = time.time()

        # at the very begining when no process has arrived
        process_queue = Queue();
        self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)

        while len(process_table):
            self.proc_rr_loop(process_table, process_queue, memory_pool, placement_algo)
        print "time %sms: Simulator for RR and %s ended"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, placement_algo)
        # stat
        burst_num = 0
        for val in self.process_table.values():
            avg_burst_time += (val['burst_time']*val['num_burst'])
            burst_num += val['num_burst']
        self.avg_turnaround_time = (self.avg_wait_time + self.t_cs*self.switch_count + avg_burst_time + self.t_pseudo_elapsed)/burst_num
        avg_burst_time /= burst_num
        self.avg_wait_time /= burst_num
        print "Algorithm RR and %s"%placement_algo
        print "-- average CPU burst time: %.2f ms"%avg_burst_time
        print "-- average wait time: %.2f ms"%self.avg_wait_time
        print "-- average turnaround time: %.2f ms"%self.avg_turnaround_time
        print "-- total number of context switches: %s"%self.switch_count

    def proc_srt_loop(self, process_table, process_queue, memory_pool, placement_algo, current_process=None):
        if process_queue.isEmpty(): # the CPU is idle
            self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)
            self.poll_io_srt(process_table, process_queue) # preemption makes no sense in this case
        else:
            # context switch: the process of storing and restoring the state (more specifically, the execution context) of a process
            # poll io performance
            if not current_process:
                current_process = process_queue.delMin()
                self.avg_wait_time += (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
            t1 = time.time()
            while int(1000*(time.time() - t1)) < self.t_cs:
                self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)
                self.poll_io_srt(process_table, process_queue) # preemption makes no sense in this case ???
            self.switch_count += 1

            # switch to the next process
            process_table[current_process]['status'] = 1 # actively using the CPU
            process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
            print "time %sms: Process '%s' started using the CPU [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue.keys()))[1:]

            while int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'] < process_table[current_process]['next_burst_time']:
                self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)
                ret_val = self.poll_io_srt(process_table, process_queue, current_process) # poll io performance, preemption may occur
                if not ret_val == -1: # a preemption has occurred
                    # context switch
                    # poll io performance
                    self.proc_srt_loop(process_table, process_queue, memory_pool, placement_algo, ret_val)
                    return
            # handle CPU
            if process_table[current_process]['num_burst'] == 1: # it is the last CPU burst
                # recycle memory
                self.recycle_memory(memory_pool, current_process)
                del process_table[current_process] # delete the completed process
                print "time %sms: Process '%s' terminated [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue.keys()))[1:]
            else:
                print "time %sms: Process '%s' completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue.keys()))[1:]
                process_table[current_process]['next_burst_time'] = process_table[current_process]['burst_time']
                if process_table[current_process]['io_time'] > 0:
                    print "time %sms: Process '%s' performing I/O [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue.keys()))[1:]
                    process_table[current_process]['status'] = 2 # blocked on (or performing) I/O
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                else:
                    process_table[current_process]['status'] = 0 # ready to use the CPU
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                    process_table[current_process]['num_burst'] -= 1
                    process_queue.insert(current_process, process_table[current_process]['next_burst_time'])

    def proc_rr_loop(self, process_table, process_queue, memory_pool, placement_algo):
        if len(process_queue) == 0: # the CPU is idle
            self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)
            self.poll_io_rr(process_table, process_queue)
        else:
            # context switch: the process of storing and restoring the state (more specifically, the execution context) of a process
            # poll io performance
            current_process = process_queue.popleft()
            self.avg_wait_time += (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
            t1 = time.time()
            while int(1000*(time.time() - t1)) < self.t_cs:
                self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)
                self.poll_io_rr(process_table, process_queue)

            self.switch_count += 1
            # switch to the next process
            process_table[current_process]['status'] = 1 # actively using the CPU
            process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
            t = time.time()
            print "time %sms: Process '%s' started using the CPU [Q "%(int(1000*(t-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue))[1:]
            while int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'] < process_table[current_process]['next_burst_time']:
                if len(process_queue) and int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'] > self.t_slice: # at least one process in the ready queue and slice time out
                    # preemption occurs
                    current_proc_remaining_time = process_table[current_process]['next_burst_time'] - \
                            (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
                    process_table[current_process]['status'] = 0 # ready to use the CPU
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                    process_table[current_process]['next_burst_time'] = current_proc_remaining_time
                    process_queue.insert(current_process) # run the remaining time next round
                    print "time %sms: Process '%s' preempted due to time slice expiration [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue))[1:]
                    return
                self.new_arrival_proc(process_table, process_queue, memory_pool, placement_algo)
                self.poll_io_rr(process_table, process_queue) # poll io performance
            # handle CPU
            if process_table[current_process]['num_burst'] == 1: # it is the last CPU burst
                self.recycle_memory(memory_pool, current_process)
                del process_table[current_process] # delete the completed process
                print "time %sms: Process '%s' terminated [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue))[1:]
            else:
                if process_table[current_process]['io_time'] > 0:
                    print "time %sms: Process '%s' completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue))[1:]
                    print "time %sms: Process '%s' performing I/O [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue))[1:]
                    process_table[current_process]['status'] = 2 # blocked on (or performing) I/O
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                else:
                    process_table[current_process]['status'] = 0 # ready to use the CPU
                    process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                    process_table[current_process]['num_burst'] -= 1
                    print "time %sms: Process '%s' completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process) + ("%s"%list(process_queue))[1:]
                    process_queue.insert(current_process)

    def poll_io_srt(self, process_table, process_queue, current_process=None):
        # check IO performance (two processes complete io at the same time?)
        preemption_flag = False # preemption ties are broken using process number order
                                # preemption occurs at most once in an io poll
        preemption_process = -1
        for k, v in process_table.items():
            if v['status'] == 2: #blocked on (or performing) I/O
                if int(1000*(time.time()-self.t0)) - v['start_time'] >= v['io_time']:
                    process_table[k]['num_burst'] -= 1
                    # preemptive
                    if not current_process or preemption_flag:
                        process_table[k]['status'] = 0 # ready to use the CPU
                        process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                        process_queue.insert(k, process_table[k]['next_burst_time'])
                        print "time %sms: Process '%s' completed I/O [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, k) + ("%s"%list(process_queue.keys()))[1:]
                    else:
                        current_proc_remaining_time = process_table[current_process]['next_burst_time'] - \
                                (int(1000*(time.time()-self.t0)) - process_table[current_process]['start_time'])
                        if process_table[k]['next_burst_time'] < current_proc_remaining_time:
                            # a preemption occurs
                            print "time %sms: Process '%s' completed I/O [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, k) + ("%s"%list(process_queue.keys()))[1:]
                            process_table[current_process]['status'] = 0 # ready to use the CPU
                            process_table[current_process]['start_time'] = int(1000*(time.time()-self.t0))
                            process_table[current_process]['next_burst_time'] = current_proc_remaining_time
                            process_queue.insert(current_process, current_proc_remaining_time) # run the remaining time next round
                            process_table[k]['status'] = -1 # to avoid being polled io again
                            preemption_flag = True
                            preemption_process = k
                            print "time %sms: Process '%s' preempted by Process '%s' [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, current_process, k) + ("%s"%list(process_queue.keys()))[1:]
                        else:
                            process_table[k]['status'] = 0 # ready to use the CPU
                            process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                            process_queue.insert(k, process_table[k]['next_burst_time'])
                            print "time %sms: Process '%s' completed I/O [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, k) + ("%s"%list(process_queue.keys()))[1:]
        return preemption_process

    def poll_io_rr(self, process_table, process_queue):
        # check IO performance (two processes complete io at the same time?)
        for k, v in process_table.items():
            if v['status'] == 2: #blocked on (or performing) I/O
                if int(1000*(time.time()-self.t0)) - v['start_time'] >= v['io_time']:
                    process_table[k]['status'] = 0 # ready to use the CPU
                    process_table[k]['start_time'] = int(1000*(time.time()-self.t0))
                    process_table[k]['num_burst'] -= 1
                    print "time %sms: Process '%s' completed I/O [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, k) + ("%s"%list(process_queue))[1:]
                    process_queue.insert(k)

    def new_arrival_proc(self, process_table, process_queue, memory_pool, placement_algo):
        """
        handle new incomming processes
        """
        for proc_num, values in process_table.iteritems():
            if values['status'] == 3 and values['arrival_time'] <= int(1000*(time.time()-self.t0)):
                # allocatinig memory for the process
                ret = self.memory_placement(memory_pool, [proc_num, values['memory']], placement_algo)
                memory_graph = self.draw_mem_graph(memory_pool)
                if ret == -1: # no suitable free partition is available
                    print "time %sms: Process '%s' unable to be added; lack of memory"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, proc_num)
                    print "time %sms: Starting defragmentation (suspending all processes)"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed)
                    print "time %sms: Simulated Memory:"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed)
                    print memory_graph
                    # do defragmentation
                    ret, moved_units = self.defragm()
                    # simulate the elapsed time of defragmentation
                    self.t_pseudo_elapsed += self.t_memmove * moved_units
                    print "time %sms: Completed defragmentation (moved %s memory units)"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, moved_units)
                    print "time %sms: Simulated Memory:"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed)
                    print self.draw_mem_graph(memory_pool)
                    if ret == 0:
                        process_queue.insert(proc_num, values['burst_time']) # insert proc_num: cpu burst time
                        process_table[proc_num]['status'] = 0 # ready to use the CPU
                        print "time %sms: Process '%s' added to system [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, proc_num) + ("%s"%list(process_queue.keys()))[1:]
                    else:
                        raise "time %sms: defragmentation failed!"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed)
                        # to do
                else:
                    process_queue.insert(proc_num, values['burst_time']) # insert proc_num: cpu burst time
                    process_table[proc_num]['status'] = 0 # ready to use the CPU
                    print "time %sms: Process '%s' added to system [Q "%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed, proc_num) + ("%s"%list(process_queue.keys()))[1:]
                    print "time %sms: Simulated Memory:"%(int(1000*(time.time()-self.t0)) + self.t_pseudo_elapsed)
                    print memory_graph

    def memory_placement(self, memory_pool, proc_info, algo):
        _start = 0
        last_used = -1
        ret = -1
        if algo == 'FirstFit':
            for values in memory_pool.locator.values():
                if values['start'] - _start >= proc_info[1]:
                    memory_pool.locator[proc_info[0]] = {'start':_start, 'size':proc_info[1]}
                    ret = 0
                    break
                else:
                    _start = values['start'] + values['size']
            if ret == -1:
                last_used = values['start'] + values['size'] - 1 if len(memory_pool.locator) else -1
                if memory_pool.units_per_line * memory_pool.line_num - last_used -1 >= proc_info[1]:
                    memory_pool.locator[proc_info[0]] = {'start': last_used + 1, 'size': proc_info[1]}
                    ret = 0
        elif algo == 'NextFit':
            if memory_pool.recent_proc and \
                memory_pool.locator.has_key(memory_pool.recent_proc):
                # if the most recently placed process has been terminated
                # (i.e., the corresponding memory has been recycled,
                # then we use FirstFit algo)
                recent = memory_pool.locator[memory_pool.recent_proc]
                _start = recent['start'] + recent['size']
            for values in memory_pool.locator.values():
                if values['start'] <= _start:
                    continue
                if values['start'] - _start >= proc_info[1]:
                    memory_pool.locator[proc_info[0]] = {'start':_start, 'size':proc_info[1]}
                    memory_pool.recent_proc = proc_info[0]
                    ret = 0
                    break
                else:
                    _start = values['start'] + values['size']
            if ret == -1:
                last_used = values['start'] + values['size'] - 1 if len(memory_pool.locator) else -1
                if memory_pool.units_per_line * memory_pool.line_num - last_used -1 >= proc_info[1]:
                    memory_pool.locator[proc_info[0]] = {'start': last_used + 1, 'size': proc_info[1]}
                    memory_pool.recent_proc = proc_info[0]
                    ret = 0
        elif algo == 'BestFit':
            best_start = 0
            min_internal_fragm = float('inf')
            for values in memory_pool.locator.values():
                internal_fragm = values['start'] - _start - proc_info[1]
                if internal_fragm >= 0:
                    if min_internal_fragm > internal_fragm:
                        min_internal_fragm = internal_fragm
                        best_start = _start
                    ret = 0
                _start = values['start'] + values['size']
            if ret == -1:
                last_used = values['start'] + values['size'] - 1 if len(memory_pool.locator) else -1
                if memory_pool.units_per_line * memory_pool.line_num - last_used -1 >= proc_info[1]:
                    memory_pool.locator[proc_info[0]] = {'start': last_used + 1, 'size': proc_info[1]}
                    ret = 0
            else:
                memory_pool.locator[proc_info[0]] = {'start': best_start, 'size': proc_info[1]}
        else:
            raise ValueError('invalid memory placement arg: %s'%algo)

        if ret == 0: # reorder
            memory_pool.locator = OrderedDict(sorted(memory_pool.locator.iteritems(), key=lambda d:d[1]['start']))
        return ret

    def defragm(self, memory_pool, proc_info):
        _start = 0
        ret = -1
        moved_units = 0
        for proc_num, values in memory_pool.locator.items():
            if memory_pool.locator[proc_num]['start'] > _start:
                memory_pool.locator[proc_num]['start'] = _start
                moved_units += values['size']
            _start = memory_pool.locator[proc_num]['start'] + values['size']
        # allocate memory for the new process
        if memory_pool.units_per_line * memory_pool.line_num - _start >= proc_info[1]:
            memory_pool.locator[proc_info[0]] = {'start': _start, 'size': proc_info[1]}
            # reorder
            memory_pool.locator = OrderedDict(sorted(memory_pool.locator.iteritems(), key=lambda d:d[1]['start']))
            ret = 0
        return ret, moved_units

    def recycle_memory(self, memory_pool, proc_num):
        del memory_pool.locator[proc_num]

    def draw_mem_graph(self, memory_pool):
        # draw memory graph
        body = [ '.' for x in range(memory_pool.line_num * memory_pool.units_per_line)]
        # body = [memory_pool.units_per_line * '.' for x in range(memory_pool.line_num)]
        for proc_num, values in memory_pool.locator.items():
            body[values['start'] : values['start'] + values['size']] = [str(proc_num) for x in range(values['size'])]

        body2 = []
        for i in range(memory_pool.line_num):
            body2.extend(body[i * memory_pool.units_per_line: (i + 1) * memory_pool.units_per_line] + ['\r\n'])
        body2 = ''.join(body2)
        memory_graph = memory_pool.units_per_line * '=' + '\r\n' + \
                            body2 + memory_pool.units_per_line * '='
        return memory_graph


class MemPool(object):
    def __init__(self, units_per_line, line_num):
        super(MemPool, self).__init__()
        self.units_per_line = units_per_line
        self.line_num = line_num
        self.locator = OrderedDict()
        self.recent_proc = None

class Queue(deque):
    def __init__(self):
        super(Queue, self).__init__()

    def insert(self, k, v=None):
        """
        just for compatibility
        """
        self.append(k)

    def keys(self):
        return list(self)

if __name__ == '__main__':
    import sys
    bos = BaseOS()
    try:
        in_file = sys.argv[1]
    except:
        in_file = 'processes.txt'
        # print "ERROR: please input the filename"
        # exit()
    bos.load_process(in_file)
    bos.run_proc('SRT', 'FirstFit')
    print '\n'
    bos.run_proc('SRT', 'NextFit')
    print '\n'
    bos.run_proc('SRT', 'BestFit')
    print '\n'
    bos.run_proc('RR', 'FirstFit')
    print '\n'
    bos.run_proc('RR', 'NextFit')
    print '\n'
    bos.run_proc('RR', 'BestFit')
