"""
Performs parallelization via the Dask scheduler
"""

from dask import delayed, compute, persist
import dask.bag as dbag
from dask.distributed import Client, progress, LocalCluster, wait, TimeoutError
from .core import ProcManager, _empty_func, function_maker
import numpy as np
import sys
from ..helpers import find_missing_params
import time

__all__ = ['DaskManager']


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
        timeout : int, optional
            How long in seconds to wait for

        Returns
        -------
        A list of (parameters, func(parameters)) for each parameter set in
        `params`. Note: this list is unordered (based on when popped off of the
        queue)
        """
        # Check that we haven't shut down
        assert self._runnable, "Cannot run_batch on shut down DaskManager"
        assert display_progress in ['none', 'bar']

        if display_progress == 'bar' and timeout is not None:
            warnings.warn("timeout does not work with progress display")

        n_params = len(params)
        #bag = dbag.from_sequence(params)

        #fh = delayed(self.fh)
        fh = self.fh
        tocalc = [self.client.submit(fh, p, pure=False) for p in params]
        #tocalc = bag.map(delayed(fh))
        #import pdb; pdb.set_trace()
        rlist = persist(*tocalc)
        if display_progress == 'bar':
            progress(rlist)
        running = True
        ret = []
        while running:
            try:
                wait(rlist, timeout=timeout)
            except TimeoutError as e:
                pass # We expect to timeout sometimes
            except Exception as e:
                raise e
            running = False
            #print([f.status for f in rlist])
            for f in rlist:
                if f.status == 'finished':
                    ps, r = f.result()
                    if isinstance(r, Exception):
                        print("Error:")
                        print(r)
                        print("Parameters:")
                        print(ps)
                        print('--------------')
                        if hard_error:
                            raise r
                    else:
                        ret.append([ps, r])
                elif f.status == 'error':
                    print("Running error:")
                    print(f.traceback())
                    print('-------------')
                    if hard_error:
                        raise f.exception()
                    else:
                        running = True
                elif f.status == 'pending':
                    print("Timeout error")
                    if hard_error:
                        raise TimeoutError
                    else:
                        running = True
            running = running and retry_failures
            if running:
                # Find the stuff we still need to do
                uparams = [p for p, r in ret]
                missing_params = find_missing_params(params, uparams)
                [f.cancel() for f in rlist]
                print("Restarting " + str(len(missing_params)) + " parameters")
                tocalc = [self.client.submit(fh, p, pure=False)
                          for p in missing_params]
                rlist = persist(*tocalc)
        sys.stdout.flush() # Push everything out (helps with clusters)
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
