"""
Implements grid search for naive fitting
"""

import numpy as np
from scipy.optimize import OptimizeResult

__all__ = ['grid_search']


def grid_search(func, xs, args=(), options={}, callback=None):
    """
    Optimize with naive grid search in a way that outputs an OptimizeResult

    Parameters
    ----------
    func : function
        the function to optimize
    xs : list
        a list of 1d arrays that comprise the parameters to run over the grid
    args : dict
        a list of default arguments to give to func (beyond the parameters)
    callback : function
        optional function that is called after each iteration. uses the call
        callback(xr, res) where xr is the parameter vector and res is the
        function result

    Returns
    -------
    opt : OptimizeResult
        an optimization result similar to scipy.optimize.minimize
    """

    disp = options.get('disp', False)

    best_res = None
    best_pars = None
    nfev = 0

    for x0 in xs:
        res = func(x0, *args)
        nfev += 1
        if res is not None and res < best_res:
            best_res = res
            best_pars = x0
        if callback:
            callback(x0, res)

    opt = OptimizeResult(x=best_pars, fun=best_res,
                         nfev=nfev, success=True,
                         status=0, message="Grid complete")
    return opt
