"""
Base class for calculating objective functions in a distributed fashion

Splits the functions into two types: data processing (can be distributed) and
generating the objective function from that data. For instance, running a model
can be expensive and parallelized, but calculating the likelihood of observing
that data from the model is cheaper and doesn't require parallelization

All models used in the COEP optimizer should inheret from this class, and
"""

from .managers import ProducerManager, DaskManager, SingleThreadManager
import time

def default_objective(processed_data, **aux_params):
    """
    Takes the processed data to calculate the objective to minimize

    Parameters
    ----------
    processed_data : list
        A list of [(params, process_data(**params)), ...] that are the
        result of applying the process_data method across all parameters
    **aux_params
        Any other parameters can be defined and passed through the COEP
        object

    Returns
    -------
    A single number representing the objective to minimize
    """
    tot = 0
    for params, obj in processed_data:
        tot += obj
    return tot

class ObjectiveProcessor:
    """
    The base class to inheret from
    """

    def __init__(self, process_func, objective_func, fit_parameter_names,
                 instance_set, num_processes, aux_process_params={},
                 aux_obj_params={}, fit_param_transform=None,
                 manager_type="dask", dask_cluster=None):
        """
        Initialize the ObjectiveProcessor

        Starts up a ProcManager to handle parallelization

        Parameters
        ----------
        process_func : function
            The function that will be called on individual instances to
            produce processed data
        objective_func : function
            The function that takes in the output from running process_func
            across all instance sets and returns a single number for the
            objective function
        fit_parameter_names : list of strings
            The names of the parameters that will get passed to the
            optimization function
        instance_set : list of parameter dictionaries
            A set of parameters that will be run with the same optimization
            parameters (e.g., trial names)
        num_processes : int
            The number of processes to spread parallalelization across
        aux_process_params : dict, optional
            Auxiliary parameters that get passed to the function when
            processing data. Defaults to {}
        aux_obj_params : dict, optional
            Auxiliary parameters that get passed to the objective function.
            Defaults to {}
        fit_param_transform : function, optional
            A function that takes in two arguments: a list of parameter values
            and the fit_parameter_names and returns a list of transformed
            parameter values. This is used to help stabilize optimization.
            Defaults to doing nothing
        manager_type : ['dask', 'singlethread'], optional
            Which type of manager to use defaults to 'dask'.
        dask_cluster : dask_jobqueue cluster type
            Only used if using a DaskManager. Defaults to LocalCluster()
        """
        self.n_proc = num_processes
        self.instance_set = instance_set
        self.pnames = fit_parameter_names
        self.aux_proc = aux_process_params
        self.aux_obj = aux_obj_params

        self.param_transform = fit_param_transform

        self.proc_f = process_func
        self.obj_f = objective_func

        if manager_type == 'dask':
            self.pm = DaskManager(self.proc_f, num_processes,
                                  dask_cluster)
        elif manager_type == 'singlethread':
            self.pm = SingleThreadManager(self.proc_f)
        else:
            raise Exception("Illegal manager_type given: " + manager_type)

    def process_all_data(self, fitting_params, display_progress="none",
                         hard_error=False, retry_failures=False, timeout=None):
        assert len(fitting_params) == len(self.pnames), "Malformed parameters"
        # Make the parameters to feed into the ProducerManager
        if self.param_transform is not None:
            fitting_params = self.param_transform(fitting_params,
                                                  self.pnames)
        this_batch = []
        parameter_set = dict(zip(self.pnames, fitting_params))
        for inst in self.instance_set:
            # Start with the specific instances to spread over
            b = inst.copy()
            # Add the fitting parameters
            b.update(parameter_set)
            # Add any auxiliary arguments
            b.update(self.aux_proc)
            this_batch.append(b)
        # Run in a parallelized fashion
        processed = self.pm.run_batch(this_batch, display_progress,
                                      hard_error=hard_error,
                                      retry_failures=retry_failures,
                                      timeout=timeout)
        # Clean up the return to include only the instance set parameters
        ret = []
        strip_names = self.pnames + list(self.aux_proc.keys())
        for pset, r in processed:
            clean_params = {}
            for k, v in pset.items():
                if k not in strip_names:
                    clean_params[k] = v
            ret.append((clean_params, r))
        return ret

    def calculate_objective(self, processed_data, **aux_params):
        return self.obj_f(processed_data, **aux_params)

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.shut_down()

    def shut_down(self):
        self.pm.shut_down()
