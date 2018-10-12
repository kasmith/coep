"""
Sample code for setting up COEP to do multivariate regression

Uses a relatively trivial case to show how to use the COEP and
ObjectiveProcessor objects

Note that this takes much longer than running without the paralleliztion...
it's just an example
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Up 1 level
from coep import COEP, ObjectiveProcessor, SPSA, default_objective
import numpy as np
from scipy.stats import norm, linregress
from scipy.optimize import minimize

import pdb

np.random.seed(10101)
# Make the regression points
def gen_reg(x1, x2):
    return 1 - 2*x1 + 3*x2 + .5*x1*x2 + norm.rvs(scale=2)
xs = norm.rvs(scale=5, size=(20,2))
ys = np.array([gen_reg(x1, x2) for x1, x2 in xs])

# Define the class to inheret from ObjectiveProcessor
def regression_func (x1, x2, y, b0, b1, b2, b12):
    pred = b0 + b1*x1 + b2*x2 + b12*x1*x2
    err = pred - y
    return err*err

if __name__ == '__main__':

    parameter_names = ['b0', 'b1', 'b2', 'b12']
    observations = [{'x1':x1, 'x2':x2, 'y':y} for (x1, x2), y in zip(xs, ys)]
    num_proc = 2

    with ObjectiveProcessor(regression_func, default_objective, parameter_names,
                            observations, 2) as objproc:
        x0 = np.array([0., 0., 0., 0.])
        '''
        # Option 1: nelder-mead
        coep = COEP(objproc, minimize)
        o = coep.optimize(x0, solver_settings={'method': 'Nelder-Mead'}, options={'disp': True})
        print(o)
        '''
        # Option 2: SPSA
        coep = COEP(objproc, SPSA, dbname="tmpdb.hdf5")
        o = coep.optimize(x0, solver_settings={'a_par': 0.0005, 'c_par': 0.1, 'xtol': 0.0000001},
                          options={'disp': True, 'savestate': 'tmpstate.json', 'maxiter': 250})
        print(o)
