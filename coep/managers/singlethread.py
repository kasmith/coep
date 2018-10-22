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

    def run_batch(self, params, display_progress="none", hard_error=True):
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

        Returns
        -------
        A list of (parameters, func(parameters)) for each parameter set in
        `params`. Note: this list is unordered (based on when popped off of the
        queue)
        """
        # Check that we haven't shut down
        assert display_progress in ['none', 'bar']

        n_params = len(params)
        ret = []
        if display_progress == 'bar':
            progress_bar(0, n_params)

        for i, p in enumerate(params):
            if hard_error:
                ret.append(self.fh(p))
            else:
                try:
                    ret.append(self.fh(p))
                except Exception as e:
                    print("Exception found")
                    print("Parameters:", p)
                    print("Error:", e)
            if display_progress == 'bar':
                progress_bar(i+1, n_params)
        return ret
