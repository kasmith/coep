"""
Base class for calculating objective functions in a distributed fashion

Splits the functions into two types: data processing (can be distributed) and
generating the objective function from that data. For instance, running a model
can be expensive and parallelized, but calculating the likelihood of observing
that data from the model is cheaper and doesn't require parallelization

All models used in the COEP optimizer should inheret from this class, and
"""

from .producer import ProducerManager
import time

class ObjectiveProcessor:
    """
    The base class to inheret from
    """

    def __init__(self, parameter_names, instance_set, num_processes,
                 aux_process_params={}):
        """
        Initialize the ObjectiveProcessor

        Starts up a ProducerManager that runs the
        """
        self.n_proc = num_processes
        self.instance_set = instance_set
        self.pnames = parameter_names
        self.aux_proc = aux_process_params

        self.pm = ProducerManager(self.process_data, self.n_proc,
                                  self.initialize_process, True)

    def process_all_data(self, fitting_params, display_progress="none",
                         hard_error=False):
        assert len(fitting_params) == len(self.pnames), "Malformed parameters"
        # Make the parameters to feed into the ProducerManager
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
                                      hard_error=hard_error)
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

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.shut_down()

    def shut_down(self):
        self.pm.shut_down()


    def process_data(self, **parameters):
        """
        The function that can take in a set of parameters to produce a part of
        the data required to form the likelihood

        NOTE: Must be overwritten by subclass
        """
        raise NotImplementedError("Must overwrite with inheriting class")

    def initialize_process(self):
        """
        A function that produces a dictionary of data that will also be used to
        support the process_data function. Can be useful if there is costly
        preprocessing required, but is run once per Producer used

        Defaults to returning an empty dictionary
        """
        return {}

    def calculate_objective(self, processed_data, **aux_params):
        """
        Takes the processed data to calculate the objective to minimize

        NOTE: should in general be overwritten -- by default just sums the
        split objective functions

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
