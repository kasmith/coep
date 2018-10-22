"""
Defines the core ProcManager class that other managers extend from
"""

def _empty_func():
    """Default initialization function"""
    return {}

def function_maker(func):
    """Wraps a function to return [params, f(params)]"""
    def f(params):
        return [params, func(**params)]
    return f

class ProcManager:
    """
    A holder for any Manager instance that decides how to split up
    embarassiingly parallel jobs
    """

    def __init__(self, func, n_proc):
        """
        Core initialization

        Parameters
        ----------
        func : function
            The function that will be parallelized
        n_proc : int
            The number of processes to spread this across
        """
        self.func = func
        self.n_proc = n_proc
        self._runnable = True

    def __del__(self):
        self.shut_down()

    def shut_down(self):
        """
        Just unsets runnable, but used to gracefully exit processes

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self._runnable = False

    def run_batch(self, params, display_progress="none", hard_error=True):
        """
        Not yet implemented -- placeholder for child classes

        Parameters
        ----------
        params : list-like
            A list of parameter sets that will be run individually through the
            function calls
        display_progress : string
            Shows progress (if not "none"). Allows "bar" (displays terminal
            bar) or "remaining" (prints the remaining instances)
        hard_error : bool, optional
            Defines whether an error in the subprocess should crash the
            full run. Defaults to True. If False, assigns None to the result

        Returns
        -------
        Throws a NotImplementedError
        """
        raise NotImplementedError("Must extend from ProcManager")
