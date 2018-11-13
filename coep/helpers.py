# -*- coding: utf-8 -*-
"""
Functions for misc. tasks related to COEP
"""

import inspect
from functools import wraps
import signal

__all__ = ['progress_bar', 'deadline', 'TimedOutExc']


def progress_bar(prog, total, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar

    Parameters
    ----------
    prog : int
        Progress (iterations) so far
    total : int
        Total number of iterations needed
    length : int
        Character length of bar
    fill : str
        The character to print in the bar
    """
    fill_len = int(length * prog // total)
    bar = fill * fill_len + '-' * (length - fill_len)
    print('\r%s| (%s / %s)' % (bar, prog, total), end = '\r')
    # Print New Line on Complete
    if prog == total:
        print()


class TimedOutExc(Exception):
    pass


def deadline(timeout, *args):
    """
    Decorator to stop a function after a set number of seconds
    From https://filosophy.org/code/python-function-execution-deadlines---in-simple-examples/
    """
    def decorate(f):
        def handler(signum, frame):
            raise TimedOutExc()

        def new_f(*args):
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(timeout)
            return f(*args)
            signal.alarm(0)

        new_f.__name__ = f.__name__
        return new_f
    return decorate


def find_missing_params(complete_set, to_check):
    """Finds the parameters from complete_set that are not in to_check"""
    remaining = []
    for pset in complete_set:
        if pset not in to_check:
            remaining.append(pset)
    return remaining
