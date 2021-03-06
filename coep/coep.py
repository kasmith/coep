"""
The COEP class is used as the controller for all of the optimization and
function calling
"""

from .database import *
from scipy.optimize import minimize
import os
import time

import pdb

__all__ = ['COEP']


def _sum_func(dat):
    """Helper function to sum the output of the data_functions"""
    tot = 0
    for params, obj in dat:
        tot += obj
    return tot

def _empty_post_cb(params, rdata, oval, oproc):
    return

class COEP:
    """
    Constrained Optimiation with Embarassing Parallelism

    This class holds all of the information required to run optimization over
    an embarassiingly parallel function call.
    """

    def __init__(self, objprocessor, optimizer=minimize, dbname=None,
                 poststep_cb=None, force_overwrite=False,
                 hard_error=False, retry_failures=False, timeout=None):
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
        poststep_cb : function, optional
            Optional function that is run after each function step, getting the
            arguments (new parameters, processed data, objective value, oproc)
        force_overwrite : bool, optional
            If set to True, does not ask whether to overwrite an existing
            database
        hard_error : bool, optional
            Raise error if it comes up in process function; defaults to False
        retry_failures : bool, optional
            If a single function call throws an error or times out, try again?
            Defaults to False
        timeout : int, optional
            How long to wait before scrapping existing calls and starting over
            (or quitting). Defaults to no timeout
        """
        if poststep_cb is None:
            poststep_cb = _empty_post_cb
        self.oproc = objprocessor
        self.ofunc = optimizer
        self.dbname = dbname
        self.post_cb = poststep_cb

        self._he = hard_error
        self._rf = retry_failures
        self._to = timeout

        # Make the database
        if self.dbname is not None:
            if os.path.exists(self.dbname):
                choice = ""
                if force_overwrite:
                    choice = 'y'
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
    def run_step(self, params, aux_params={}, display_progress="none"):
        starttime = time.time()
        rdata = self.oproc.process_all_data(params, display_progress,
                                            hard_error=self._he,
                                            retry_failures=self._rf,
                                            timeout=self._to)
        oval = self.oproc.calculate_objective(rdata, **aux_params)
        runtime = time.time() - starttime
        if self.dbname is not None:
            write_function_call(self.dbname, params, rdata, oval, str(runtime))
        self.post_cb(params, rdata, oval, self.oproc)
        return oval


    """
    Callback to pass to solver to record data
    """
    def _solver_cb(self, params, result=None):
        if self.dbname is not None:
            write_solver_iteration(self.dbname, params, result)

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
    def optimize(self, x0, args=None, bounds=None, constraints=None, options=None,
                 solver_settings={}):
        if options is not None:
            solver_params = {'options': options}
        else:
            solver_params = {}
        if bounds is not None:
            solver_params['bounds'] = bounds
        if constraints is not None:
            solver_params['constraints'] = constraints
        solver_params['x0'] = x0
        if args is not None:
            solver_params['args'] = args
        solver_params.update(solver_settings)

        # Set up the optimization in the database
        if self.dbname is not None:
            write_optimization_initialization(self.dbname, x0, solver_params)

        opt_result = self.ofunc(self.run_step,
                                callback=self._solver_cb, **solver_params)

        if self.dbname is not None:
            write_optimization_result(self.dbname, opt_result)

        return opt_result
