# -*- coding: utf-8 -*-
"""
Functions for misc. tasks related to COEP
"""

import inspect
from functools import wraps

__all__ = ['progress_bar']


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
