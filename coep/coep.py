"""
The COEP class is used as the controller for all of the optimization and
function calling
"""

from multiprocessing import Condition, Queue, cpu_count
from .producer import Producer
from .database import *
from scipy.optimize import fmin, minimize
import os

import pdb

__all__ = ['COEP']


def _sum_func(dat):
    """Helper function to sum the output of the data_functions"""
    tot = 0
    for params, obj in dat:
        tot += obj
    return tot

class COEP:
    """
    Constrained Optimiation with Embarassing Parallelism

    This class holds all of the information required to run optimization over
    an embarassiingly parallel function call.
    """

    def __init__(self, objprocessor, optimizer=minimize, dbname=None):
        """
        Start up the COEP object

        Parameters
        ----------
        objprocessor : ObjectiveProcessor
            An object that inherets from an ObjectiveProcessor to define the
            processing and objective functions
        optimizer : function
            A function in the vein of those from scipy.optimize. Must take in
            the same arguments and return an OptimizeResult
        dbname : string
            Filepath to a database to write to. Defaults to None. If db does
            not yet exist, creates it

        """
        self.oproc = objprocessor
        self.ofunc = optimizer
        self.dbname = dbname

        # Make the database
        if self.dbname is not None:
            if os.path.exists(self.dbname):
                choice = ""
                while choice.lower() not in ['y', 'n']:
                    choice = input("Database " + self.dbname +
                                   " exists. Overwrite? [y/n] ")
                if choice.lower() == 'n':
                    self.dbname = None
                    print("Removing database writing functionality")
                else:
                    print("Overwriting database")
                    make_db(self.dbname, self.oproc, self.ofunc)
            else:
                make_db(self.dbname, self.oproc, self.ofunc)

    """
    Run a single step to get an objective function

    Parameters
    ----------
    params : 1d array
        A 1d numpy array of the parameters in the same order as the
        ObjectiveProcessor parameter_names
    aux_params : dict
        Set of parameters to pass to the calculate_objective function of
        the objprocessor
    display_progress : bool
        Flag to show progress in the terminal

    Returns
    -------
    A single objective function. Also writes output to the db in the process
    """
    def run_step(self, params, aux_params={}, display_progress=False):
        rdata = self.oproc.process_all_data(params, display_progress)
        oval = self.oproc.calculate_objective(rdata, **aux_params)
        write_function_call(self.dbname, params, rdata, oval)
        return oval


    """
    Run the optimization

    Parameters
    ----------
    x0 : 1d array
        Initial guess for parameters
    args : dict
        Extra arguments passed to the calculate_objective function
    bounds : sequence or Bounds (optional)
        Bounds on variables for optimization functions with bounds
    constraints : {Constraint, dict} or List of {Constraint, dict}, optional
        Constraints definitions for COBYLA, SLSQP, trust-constr
    options : dict, optional
        A dictionary of solver options (depends on solver)
    solver_settings : dict, optional
        Other solver settings (not passed through the option parameter)

    Returns
    -------
    A scipy OptimizeResult with the result of the optimization function
    """
    def optimize(self, x0, args={}, bounds=None, constraints=None, options={},
                 solver_settings={}):
        solver_params = {'options': options}
        if bounds is not None:
            solver_params['bounds'] = bounds
        if constraints is not None:
            solver_params['constraints'] = constraints
        solver_params.update(solver_settings)

        # Set up the optimization in the database
        if self.dbname is not None:
            write_optimization_initialization(self.dbname, x0, solver_params)

        opt_result = self.ofunc(self.run_step, x0, args, **solver_params)

        if self.dbname is not None:
            write_optimization_result(self.dbname, opt_result)

        return opt_result
