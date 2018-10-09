"""
Implements the simultaneous perturbation stochastic optimization (SPSA)
optimization function

For further algorithm details, see Spall (1992): Multivariate Stochastic
Approximation Using a Simultaneous Perturbation Gradient Approximation

See www.jhuapl.edu/SPSA/ for even more details & algorithm code base
"""

import numpy as np
import copy
import json
import os
import traceback
import sys
from scipy.optimize import OptimizeResult

__all__ = ['SPSA']


def SPSA(func, x0, args=(), xtol=0.0001,options={},
         bounds=None, maxgrad=None, a_par=1e-6, c_par=0.01, alpha=0.602,
         gamma=0.101, A=None, callback=None, start_iter=0):
    """
    Optimize with Simultaneous Perturbation Stochasitc Optimization

    This function should act much like the scipy.optimize functions. See
    www.jhuapl.edu/SPSA/ for algorithm details

    Parameters
    ----------
    func : function
        the function to optimize
    x0 : array_like
        the 1d array of initial parameter estimates
    args : dict
        a list of default arguments to give to func (beyond the parameters)
    xtol : float
        tolerance in the relative change in the parameters beyond which to stop
    options : dict
        a set of options, including:
            maxiter: int of when to stop (defaults to 1000)
            disp: bool of whether to display intermediate output
            disp_iters: int of how often to display result (after N iterations)
            savestate: string of file location for saving intermediate states
                (loads from this file if it exists). Saves as a JSON file
    bounds : array_like (len(x0), 2)
        optional set of (min, max) bounds for parameters
    maxgrad : float
        optional maximum gradient that can influence parameters; prevents
        exploding gradients
    a_par : float
        parameter to adjust the change in parameters per gradient size;
        defaults to 1e-6
    c_par : float
        parameter to adjust the parameter step size; defaults to 0.01
    alpha : float
        gain control for a_par. Defaults to 0.602 (as per Spall, 2000)
    gamma : float
        gain control for c_par. Defaults to 0.101 (as per Spall, 2000)
    A : float
        Controller for chane in parameters (adjustment to a_par). Defaults to
        maxiter / 10 (as per Spall, 2000)
    callback : function
        optional function that is called after each iteration. uses the call
        callback(xr, avg_res) where xr is the parameter vector and avg_res
        is the average of y_plus and y_minus
    start_iter : int
        a way of telling the function where to start -- should usually be
        ignored but can be used when restarting optimization

    Returns
    -------
    opt : OptimizeResult
        an optimization result similar to scipy.optimize.minimize
    """

    # Set defaults
    maxiter = options.get('maxiter', 1000)
    disp = options.get('disp', False)
    disp_iters = options.get('disp_iters', 10)
    savestate = options.get('savestate', None)

    if savestate is not None and os.path.exists(savestate):
        # Initialize from the saved state
        with open(savestate, 'rU') as savefl:
            print("Found exising state: reloading")
            saved = json.load(savefl)
            n_fev = {'n': saved['n_fev']}
            n_iter = saved['n_iter']
            saved_theta = np.array(saved['saved_theta'])
            theta = np.array(saved['theta'])
            n_params = theta.shape[0]
    else:
        # Initialize from scratch
        n_fev = {'n': 0}
        n_iter = start_iter
        n_params = x0.shape[0]

        theta = x0.copy()
        if np.all(theta == 0):
            saved_theta = np.ones(n_params)*10
        else:
            saved_theta = theta * 100

    # Make the callable loss function
    def loss(ps):
        n_fev['n'] += 1
        return func(ps, *args)

    if A is None:
        A = maxiter / 10.

    if bounds is not None:
        assert bounds.shape == (n_params, 2), "Malformed parameter bounds"
        assert np.all(bounds[:, 1] > bounds[:, 0]), \
            "Upper bounds must be greater than lower"
    if maxgrad is not None:
        assert maxgrad > 0, "Must have positive maximum gradient"

    theta_diff = (np.linalg.norm(saved_theta - theta) /
                  np.linalg.norm(saved_theta))
    while (theta_diff > xtol and n_iter < maxiter):
        saved_theta = theta.copy()
        ak = a_par / (n_iter + 1 + A)**alpha
        ck = c_par / (n_iter + 1)**gamma
        # Find stochastic perturbation
        delta = np.random.randint(0, 2, n_params) * 2 - 1
        theta_plus = theta + ck * delta
        theta_minus = theta - ck * delta
        # Clip parameters
        if bounds is not None:
            np.clip(theta_plus, bounds[:, 0], bounds[:, 1], out=theta_plus)
            np.clip(theta_minus, bounds[:, 0], bounds[:, 1], out=theta_minus)
        # Find the function values
        y_plus = loss(theta_plus)
        y_minus = loss(theta_minus)
        y_diff = y_plus - y_minus
        # Ensure the gradient isn't too large
        grad_size = np.linalg.norm(y_diff)
        if maxgrad is not None:
            if grad_size > maxgrad:
                y_diff *= maxgrad / grad_size
        g_hat = y_diff / (2 * ck * delta)
        theta -= ak * g_hat
        if bounds is not None:
            np.clip(theta, bounds[:, 0], bounds[:, 1], out=theta)
        theta_diff = (np.linalg.norm(saved_theta - theta) /
                      np.linalg.norm(saved_theta))
        n_iter += 1
        avg_loss = (y_plus + y_minus) / 2
        if disp and (n_iter % disp_iters) == 0:
            print("Iteration:", n_iter)
            print("Theta:", theta)
            print("Avg loss:", avg_loss)
            print("")
        if callback:
            callback(saved_theta, avg_loss)

        # Save a state file if asked (for easy recovery)
        if savestate is not None:
            with open(savestate, 'w') as savefl:
                json.dump({
                    'n_fev': n_fev['n'],
                    'n_iter': n_iter,
                    'saved_theta': saved_theta.tolist(),
                    'theta': theta.tolist()
                }, savefl)

    if theta_diff <= xtol:
        rmess = "Stopped due to parameter change being below threshold ("
        rmess += str(theta_diff) + " vs " + str(xtol) + ")"
    elif n_iter >= maxiter:
        rmess = "Maximum number of iterations exceeded"
    else:
        rmess = "Warning! Should not be here!"

    # Save to an OptimizeResult
    fval = loss(theta)
    opt = OptimizeResult(x=theta, fun=fval,
                         nit=n_iter, nfev=n_fev['n'],
                         success=True,
                         status=0, message=rmess)
    # Remove the saved state if it exists
    if savestate is not None and os.path.exists(savestate):
        os.remove(savestate)
    return opt
