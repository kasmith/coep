"""
A manager that doesn't paralellize (for debugging purposes)
"""

from .core import ProcManager, _empty_func, function_maker
import numpy as np
import warnings
from ..helpers import progress_bar

__all__ = ['SingleThreadManager']

class SingleThreadManager(ProcManager):
    """
    A holder for functions that don't get parallelized
    """

    def __init__(self, func, n_proc=1):
        """
        Initialize and set up Dask

        Parameters
        ----------
        func : function
            The function to parallelize
        n_proc : int
            Dummy variable for compatability
        """
        if n_proc > 1:
            warnings.warn("SingleThreadManager only uses a single process")
        super().__init__(func, 1)
        self.fh = function_maker(func)

    def run_batch(self, params, display_progress="none", hard_error=True,
                  retry_failures=False, timeout=None):
        """
        Runs a set of parameters through the Dask scheduler

        Parameters
        ----------
        params : list-like
            A list of parameter sets that will be run individually through the
            Producers
        display_progress : string
            Shows progress (if not "none"). Allows "bar" (displays terminal
            bar) or "remaining" (prints the remaining instances)
        hard_error : bool, optional
            Defines whether an error in the subprocess should crash the
            full run. Defaults to True. If False, assigns None to the result
        retry_failures : bool, optional
            On a soft error, defines whether to rerun processes that cause
            the errors. Can be useful for quirky stochastic processes; not
            recommended for deterministic functions. Defaults to False

        Returns
        -------
        A list of (parameters, func(parameters)) for each parameter set in
        `params`. Note: this list is unordered (based on when popped off of the
        queue)
        """
        # Check that we haven't shut down
        assert display_progress in ['none', 'bar']

        if timeout is not None:
            raise NotImplementedError("timeout not yet implemented in SingleThreadManager")

        n_params = len(params)
        ret = []
        if display_progress == 'bar':
            progress_bar(0, n_params)

        for i, p in enumerate(params):
            ps, res = self.fh(p)
            if isinstance(res, Exception):
                print("Error:")
                print(res)
                print("Parameters:")
                print(ps)
                print('--------------')
                if hard_error:
                    raise res
                elif retry_failures:
                    params.append(p)
            else:
                ret.append([ps, res])
            if display_progress == 'bar':
                progress_bar(i+1, n_params)
        return ret
