#!/usr/bin/python

import time
from collections import deque, OrderedDict

class BaseOS(object):
    """docstring for BaseOS"""
    def __init__(self):
        super(BaseOS, self).__init__()
        self.process_table = OrderedDict() # process table
        self.process_queue = deque()
        self.t_cs = 13 # context switch cost

    def load_process(self, filename):
        try:
            with open(filename, 'r') as f:
                for each_line in f:
                    if each_line == '' or each_line[0] == '#':
                        continue
                    conf = each_line.rstrip('\n').split('|') # <proc-num>|<burst-time>|<num-burst>|<io-time>
                    # process table {'proc-num':['burst-time', 'num-burst', 'io-time', 'status', 'eachstage-start-time']}
                    self.process_table[int(conf[0])] = dict(zip(['burst_time', 'num_burst', 'io_time', 'status', 'start_time'], [int(x) for x in conf[1:]] + [0, 0]))
        except Exception, e:
            print e
            assert False
        f.close()
        # add tho process queue
        for proc_num in self.process_table.keys():
            self.process_queue.append(proc_num)
        # All "ties" are to be broken using process number order
        self.process_table = dict(sorted(self.process_table.iteritems(), key=lambda d:d[0]))

    def run(self):
        print "time 0ms: Simulator started [Q " + ("%s"%list(self.process_queue))[1:]
        self.t0 = time.time()
        while len(self.process_table):
            if len(self.process_queue) == 0: # the CPU is idle
                self.poll_io()
            else:
                # context switch: the process of storing and restoring the state (more specifically, the execution context) of a process
                # poll io performance
                t1 = time.time()
                while int(1000*(time.time() - t1)) < self.t_cs:
                    self.poll_io()
                # switch to the next process
                current_process = self.process_queue.popleft()
                self.process_table[current_process]['status'] = 1 # actively using the CPU
                self.process_table[current_process]['start_time'] = time.time()
                print "time %sms: P%s started using the CPU [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(self.process_queue))[1:]

                while int(1000*(time.time() - self.process_table[current_process]['start_time'])) < self.process_table[current_process]['burst_time']:
                    self.poll_io() # poll io performance
                # handle CPU
                if self.process_table[current_process]['num_burst'] == 1: # it is the last CPU burst
                    del self.process_table[current_process] # delete the completed process
                    print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(self.process_queue))[1:]
                else:
                    if self.process_table[current_process]['io_time'] > 0:
                        print "time %sms: P%s completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(self.process_queue))[1:]
                        print "time %sms: P%s performing I/O [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(self.process_queue))[1:]
                        self.process_table[current_process]['status'] = 2 # blocked on (or performing) I/O
                        self.process_table[current_process]['start_time'] = time.time()
                    else:
                        self.process_table[current_process]['status'] = 0 # ready to use the CPU
                        self.process_table[current_process]['num_burst'] -= 1
                        print "time %sms: P%s completed its CPU burst [Q "%(int(1000*(time.time()-self.t0)), current_process) + ("%s"%list(self.process_queue))[1:]
                        self.process_queue.append(current_process)
        print "time %sms: Simulator ended"%(int(1000*(time.time()-self.t0)))

    def poll_io(self):
        # check IO performance (two processes complete io at the same time?)
        for k, v in self.process_table.items():
            if v['status'] == 2: #blocked on (or performing) I/O
                if int(1000*(time.time() - v['start_time'])) >= v['io_time']:
                    self.process_table[k]['status'] = 0 # ready to use the CPU
                    self.process_table[k]['num_burst'] -= 1
                    if self.process_table[k]['num_burst'] > 0:
                        print "time %sms: P%s completed I/O [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(self.process_queue))[1:]
                        self.process_queue.append(k)
                    else:
                        del self.process_table[k] # delete the completed process
                        print "time %sms: P%s terminated [Q "%(int(1000*(time.time()-self.t0)), k) + ("%s"%list(self.process_queue))[1:]

if __name__ == '__main__':
    import sys
    bos = BaseOS()
    bos.load_process(sys.argv[1])
    # bos.load_process('processes.txt')
    bos.run()
