"""
Performs parallelization via the Dask scheduler
"""

from dask import delayed, compute, persist
import dask.bag as dbag
from dask.distributed import Client, progress, LocalCluster
from .core import ProcManager, _empty_func
import numpy as np

__all__ = ['DaskManager']


class FunctionHolder:
    """Helper class to avoid serialization problems with Dask"""
    def __init__(self, func):
        self.f = func

    def __call__(self, params):
        return [params, self.f(**params)]


class DaskManager(ProcManager):
    """
    A holder for functions that get parallelized via Dask
    """

    def __init__(self, func, n_proc, cluster=None):
        """
        Initialize and set up Dask

        Parameters
        ----------
        func : function
            The function to parallelize
        n_proc : int
            The number of processes
        cluster: dask_jobqueue cluster
            The cluster to run on. Must be dask.distrbuted.LocalCluster() or
            a cluster from dask_jobqueue. Defaults to the LocalCluster
        """

        super().__init__(func, n_proc)
        #import pdb; pdb.set_trace()
        if cluster is None:
            cluster = LocalCluster()
        self.cluster = cluster
        self.client = Client(cluster)
        self.fh = FunctionHolder(func)

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
        assert self._runnable, "Cannot run_batch on shut down DaskManager"
        assert display_progress in ['none', 'bar']

        n_params = len(params)
        bag = dbag.from_sequence(params)

        fh = self.fh
        tocalc = bag.map(delayed(fh))
        #import pdb; pdb.set_trace()
        rlist = persist(*tocalc)
        if display_progress == 'bar':
            progress(rlist)
        ret = compute(*rlist)
        return ret

    def shut_down(self):
        """
        Closes the cluster to exit

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.cluster.close()
        self._runnable = False
