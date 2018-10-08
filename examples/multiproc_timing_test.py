"""
Used to test relative speed-up from multiprocessing with Producers
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Up 1 level
from coep import ObjectiveProcessor
import time
import random

class Sleeper(ObjectiveProcessor):
    def process_data(self, x, i):
        to_sleep = 10 + x + i/10 + 10*random.random()
        time.sleep(to_sleep)
        return to_sleep

class Cruncher(ObjectiveProcessor):
    def process_data(self, x, i):
        numc = 1 + x + i/100 + 1*random.random()
        numc *= 100000000
        numc = int(numc)
        stime = time.time()
        [1.5 / 3.2 for _ in range(numc)]
        return time.time() - stime


if __name__ == '__main__':
    if len(sys.argv) > 1:
        num_obs = int(sys.argv[1])
        if len(sys.argv) > 2:
            num_proc = int(sys.argv[2])
        else:
            num_proc = 2
    else:
        num_obs = 4
        num_proc = 2

    parameter_names = ['x']
    observations = [{'i': 1} for i in range(num_obs)]

    with Cruncher(parameter_names, observations, num_proc) as objproc:
        start_time = time.time()
        proc_dat = objproc.process_all_data([1], "bar")
        tot_time = time.time() - start_time
        print("Ends at:", tot_time)
        core_time = sum([o[1] for o in proc_dat])
        print("Total core sleeping:", core_time)
        print("Efficiency:", core_time / tot_time)
        print("Relative efficiency:", (core_time/tot_time) / num_proc)
