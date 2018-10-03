# -*- coding: utf-8 -*-
"""
Functions for misc. tasks related to COEP
"""

from .database import write_to_db
import inspect
from functools import wraps

__all__ = ['wrap_function_in_db', 'progress_bar']

def wrap_function_in_db(func, dbname, grouping_args):
    """
    Wraps a function to write to a database during the call

    Note that in the special case that func returns a dictionary with the keys
    `Return` and `Write`, this will cause the new function to *only* return
    the `Return` key but will store the `Write` object in the database

    Parameters
    ----------
    func : function
        The function to wrap
    dbname : string
        The filepath of the database to write to
    grouping_args : list
        A list of arg names to use to define the grouping variable from the
        arguments passed to the function

    Returns
    -------
    newfunc: A function that does the same as func but also writes its output
        (though see the notes for a special case)
    """
    argspec = inspect.getfullargspec(func)

    @wraps(func)
    def newfunc(*args, callnum=0, **kwargs):
        # Make the grouping name
        gname = []
        for g in grouping_args:
            try:
                ai = argspec.args.index(g)
                gname.append(args[ai])
            except ValueError:
                try:
                    anm = kwargs[g]
                    gname.append(anm)
                except:
                    raise ValueError("Grouping arg not found in args: " + g)
        gvar = "_".join([str(g) for g in gname])
        grouping = ['calls', str(callnum), gvar]
        ret = func(*args, **kwargs)
        if (isinstance(ret, dict) and
                'Return' in ret.keys() and 'Write' in ret.keys()):
            write_to_db(dbname, grouping, ret['Write'])
            return ret['Return']
        else:
            write_to_db(dbname, grouping, ret)
            return ret
    return newfunc


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
