"""
The COEP class is used as the controller for all of the optimization and
function calling
"""

from multiprocessing import Condition, Queue, cpu_count
from .producer import Producer
from .helpers import wrap_function_in_db
from .database import make_db, write_to_db
from scipy.optimize import fmin, minimize

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
        ####
        # MAKE THE DB here
        ####

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
        #####
        # WRITE TO DB: RAW AND VALUE
        #####
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

        opt_result = self.ofunc(self.run_step, x0, args, **solver_params)

        #####
        # WRITE RESULT TO DB
        #####

        return opt_result
