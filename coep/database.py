"""
Write/read from a database to store the output of specific functions

TODO: Add ability to re-use databases for optimization
"""

import h5py
import filelock
import inspect
import os
import numpy as np

__all__ = ['make_db', 'write_optimization_initialization',
           'write_function_call', 'write_optimization_result']

def make_db(dbname, objproc, solve_func):
    """Makes the database

    Creates the database and populates it with information about the function

    Parameters
    ----------
    dbname : string
        Name of the database to make
    objproc : ObjectiveProcessor
        The inheretor from ObjectiveProcessor used in the optimization
    solve_func : function
        The function for solving the optimization problem

    Returns
    -------
    Boolean indicating file was created appropriately
    """
    with h5py.File(dbname, 'w') as f:
        g_iinf = f.create_group('InitializationInfo')
        g_orun = f.create_group('OptimizationRuns')

        g_iinf['ObjProcName'] = objproc.__class__.__name__
        g_iinf['ObjProcFile'] = os.path.realpath(inspect.getfile(objproc.__class__))
        g_iinf['SolverName'] = solve_func.__name__
        g_iinf['SolverFile'] = os.path.realpath(inspect.getfile(solve_func))
        g_iinf['ParameterNames'] = np.string_(objproc.pnames)
        g_iset = g_iinf.create_group('InstanceSet')
        _write_nested_list(g_iset, objproc.instance_set)
        #n_iset = 0
        #for iset_inst in objproc.instance_set:
        #    newg = g_iset.create_group(str(n_iset))
        #    for pnm, val in iset_inst.items():
        #        newg[pnm] = val
        #    n_iset += 1
        g_aparam = g_iinf.create_group('AuxParams')
        _write_nested_dict(g_aparam, objproc.aux_proc)
        #for pnm, val in objproc.aux_proc.items():
        #    g_aparam[pnm] = val

        g_orun['CurrentOptRun'] = 0

    return True


def write_optimization_initialization(dbname, x0, options):
    """
    Starts a new group for a single optimization run

    Parameters
    ----------
    dbname : string
        File path of the database
    x0 : 1-d array
        Initialization of the optimization
    options : dict
        Dictionary of other options sent to the optimizer
    """
    with h5py.File(dbname, 'r+') as f:
        g_on = f['OptimizationRuns/CurrentOptRun']
        opt_num = g_on.value
        g_newopt = f['OptimizationRuns'].create_group(str(opt_num))
        g_newopt['Initialization'] = x0
        _write_nested_dict(g_newopt.create_group('Options'), options)
        g_fc = g_newopt.create_group('FunctionCalls')
        g_fc['CurCall'] = 0
        g_newopt.create_group('OptimizationResult')


def write_function_call(dbname, params, processed_data, objective_val):
    """
    Writes the results of a function call to the current pointer

    Parameters
    ----------
    dbname : string
        File path of the database
    params : 1-d array
        The parameters used to instantiate this call
    processed_data : list
        List of [{INSTANCE}, OUTCOME] from process_all_data
    objective_val : number
        The objective value from calculate_objective
    """
    with h5py.File(dbname, 'r+') as f:
        g_on = f['OptimizationRuns/CurrentOptRun']
        opt_num = g_on.value
        g_curopt = f['OptimizationRuns'][str(opt_num)]
        fcall_num = g_curopt['FunctionCalls/CurCall'].value
        g_fc = g_curopt['FunctionCalls'].create_group(str(fcall_num))
        g_fc['Parameters'] = params
        _write_nested_list(g_fc.create_group("ProcessedData"), processed_data)
        g_fc['Objective'] = objective_val
        g_curopt['FunctionCalls/CurCall'][...] = fcall_num + 1

def write_optimization_result(dbname, opt_result):
    """
    Writes the results of a full optimization to the database

    Parameters
    ----------
    dbname : string
        File path of the database
    opt_result : OptimizationResult
        A scipy OptimizationResult coming from an objective solver
    """
    with h5py.File(dbname, 'r+') as f:
        g_on = f['OptimizationRuns/CurrentOptRun']
        opt_num = g_on.value
        g_res = f['OptimizationRuns'][str(opt_num)]['OptimizationResult']
        g_res['MinVal'] = opt_result.fun
        g_res['Params'] = opt_result.x
        g_res['Message'] = opt_result.message
        g_res['FuncEval'] = opt_result.nfev
        g_res['OptIters'] = opt_result.nit
        g_res['Status'] = opt_result.status
        #import pdb; pdb.set_trace()
        g_on[...] = opt_num + 1


def _write_nested_dict(dbgroup, write_dict):
    """Writes a nested dict to a database group through recursion"""
    for k, val in write_dict.items():
        if isinstance(val, dict):
            newg = dbgroup.create_group(k)
            _write_nested_dict(newg, val)
        elif isinstance(val, list) or isinstance(val, tuple):
            newg = dbgroup.create_group(k)
            _write_nested_list(newg, val)
        else:
            dbgroup[k] = val

def _write_nested_list(dbgroup, write_list):
    for i, val in enumerate(write_list):
        k = str(i)
        if isinstance(val, dict):
            newg = dbgroup.create_group(k)
            _write_nested_dict(newg, val)
        elif isinstance(val, list) or isinstance(val, tuple):
            newg = dbgroup.create_group(k)
            _write_nested_list(newg, val)
        else:
            dbgroup[k] = val
