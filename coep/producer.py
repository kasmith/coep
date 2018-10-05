"""
Holds the class that persists as a subprocess and waits for inputs to process
"""

from multiprocessing import Process, Condition, Event, Queue, Manager
import multiprocessing
from .helpers import progress_bar
import numpy as np
import sys
import os
import time
import warnings

import pdb

__all__ = ['Producer']


def _empty_func():
    """Default initialization function"""
    return {}

class Producer(Process):
    """
    The Producer class extends a multiprocessing.Process and exists to pull
    input from a queue, run a function on it, and spit back out the output
    """

    def __init__(self, func, initialization, queues, conds, seed=None):
        """
        Initializes the Producer

        Parameters
        ----------
        func : function
            the function that gets called on arguments passed
        initialization : function
            a function that gets called once topre-process inputs. Must return
            a dict that can be input to func
        queues : [Queue, Queue]
            the input/output queues for communication
        conds : [Condition, Condition]
            conditions for blocking the queues
        seed : int
            an (optional) seed to set numpy.random
        """
        super(Producer, self).__init__()
        assert callable(initialization)
        assert callable(func)
        # NOTE: this is unsafe, but class checking is odd for Manager spawns...
        #assert all([type(q) == Queue for q in queues])
        #assert all([type(c) == Condition for c in conds])
        self._f = func
        self._init = initialization
        self._set_q, self._get_q, self._err_q = queues
        self._set_cond, self._get_cond, self._err_cond = conds
        self._stop = Event()
        self._seed = seed

    def run(self):
        """
        Start running the Producer

        This must be called prior to feeding anything into the input queue, or
        the Producer won't start reading it out. Takes no arguments and returns
        nothing
        """
        # Do preprocessing
        if self._seed:
            np.random.seed(self._seed)
        else:
            np.random.seed()
        preproc_data = self._init()

        # Keep looping until told to quit
        while not self._stop.is_set():
            # Pop parameters from the queue
            try:
                self._set_cond.acquire()
                if self._set_q.empty():
                    params = None
                else:
                    params = self._set_q.get()
                self._set_cond.release()
            except (BrokenPipeError, EOFError) as e:
                warnings.warn("Broken condition pipe - shutting down; " +
                              "Use `with` contexts to avoid this warning!")
                return

            # If we got params, deal with it
            if params is not None:
                # Wrap in try to catch any errors
                try:
                    # Run the function on the parameters & preprocessed data
                    uparams = params.copy()
                    uparams.update(preproc_data)
                    ret = (params, self._f(**uparams))

                    # Put the results back onto the queue
                    self._get_cond.acquire()
                    self._get_q.put(ret)
                    self._get_cond.notify()
                    self._get_cond.release()
                # Handle errors that happen
                except Exception as err:
                    self._err_cond.acquire()
                    newerr = type(err)("Error encountered with parameters:\n" +
                                       str(params) + "\n" +
                                       str(err))
                    self._err_q.put((params, newerr))
                    self._err_cond.notify()
                    self._err_cond.release()

            # Close down if needed
            if self._stop.is_set():
                return

    def stop(self):
        """
        Stops the Producer from running

        Sets the event flag that lets the Producer know when it's time to quit.
        Takes no parameters and returns nothing
        """
        self._stop.set()


class ProducerManager:
    """
    A holder for a set of producers and a communications pipeline for pushing parameters on and reading them off
    """

    def __init__(self, func, n_producers, initialization=_empty_func,
                 set_random=True):
        """
        Initialize and start up all of the producers

        Parameters
        ----------
        func : function
            The function that the Producers will run on the parameters
        n_producers : int
            The number of producers to make
        initialization : function
            The function that is run to start up the Producer. Defaults to an
            empty function that returns an empty dict
        set_random : bool
            Ensures the Producer processes get seeded for randomness (from os)
        """
        self.m = Manager()
        self.qin = self.m.Queue()
        self.qout = self.m.Queue()
        self.qerr = self.m.Queue()
        self.cin = self.m.Condition()
        self.cout = self.m.Condition()
        self.cerr = self.m.Condition()
        self.plist = []
        self._runnable = True
        for _ in range(n_producers):
            if set_random:
                r = int.from_bytes(os.urandom(4), sys.byteorder)
            else:
                r = None
            newp = Producer(func, initialization,
                            [self.qin, self.qout, self.qerr],
                            [self.cin, self.cout, self.cerr],
                            seed=r)
            self.plist.append(newp)
            newp.start()

    def __del__(self):
        self.shut_down()

    def run_batch(self, params, display_progress=True, waittime=0.1,
                  hard_error=True):
        """
        Runs a set of parameters through the Producers

        Takes in a set of parameters and returns the output

        Parameters
        ----------
        params : list-like
            A list of parameter sets that will be run individually through the
            Producers
        display_progress : bool
            Show a bar in the terminal with progress through the parameters.
            Defaults to True
        waittime : float
            Time in seconds between checking for updates to the output queue.
            Lower values can cause slowdowns. Defaults to 0.1s
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
        assert self._runnable, "Cannot run_batch on shut down ProducerManager"
        # Shove everything into the queue
        n_params = len(params)
        self.cin.acquire()
        for p in params:
            self.qin.put(p)
        self.cin.notify()
        self.cin.release()

        # Set up return and keep checking the queue
        rlist = []
        while len(rlist) < n_params:
            self.cout.acquire()
            while not self.qout.empty():
                rlist.append(self.qout.get())
            self.cout.notify()
            self.cout.release()
            # Look for errors
            self.cerr.acquire()
            if not self.qerr.empty():
                err = self.qerr.get()
                if hard_error:
                    raise err[1]
                else:
                    print('Error found:', str(err[1]))
                    rlist.append((err[0], None))
            self.cerr.notify()
            self.cerr.release()
            if display_progress:
                progress_bar(len(rlist), n_params)
            time.sleep(waittime)
        return rlist

    def shut_down(self):
        """
        Closes down all of the producers for graceful shut-down

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        for p in self.plist:
            p.stop()
            del p
        self._runnable = False
