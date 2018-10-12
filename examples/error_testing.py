"""
Sample code to see error handling within the ObjectiveProcessor

Will throw an error on 1/4 of inputs -- can handle gracefully or crash
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Up 1 level
from coep import ObjectiveProcessor, default_objective

CRASH_ON_ERR = False

# Set the function to throw errors at certain indices
def crasher(x, idx):
    if idx % 4 == 3:
        raise Exception("Ouch! I crashed!")
    return (x+idx) * (x+idx)

if __name__ == '__main__':
    # 'x' is to be fit, we handle idx
    parameter_names = ['x']
    observations = [{'idx': i} for i in range(10)]
    num_proc = 2

    with ObjectiveProcessor(crasher, default_objective, parameter_names,
                            observations, 2) as objproc:
        proc_dat = objproc.process_all_data([1], "none",
                                            hard_error=CRASH_ON_ERR)
        print(proc_dat)
