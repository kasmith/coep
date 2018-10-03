"""
Write/read from a database to store the output of specific functions
"""

import h5py
import filelock

"""Makes the database

Stores

Args:
    ...

Returns:
    Boolean indicating file was created appropriately
"""
def make_db(dbname):
    return


"""Writes to the database

...

Args:
    dbname
    grouping
    data

Returns:
    ...
"""
def write_to_db(dbname, grouping, data):
    return

"""Reads a specific output from the database

...

Args:
    dbname
    grouping

Returns:
    The data stored in db[group1][group2][...]
"""
def read_from_db(dbname, grouping):
    return

"""Reads the core output from the database

...

Args:
    dbname

Returns:
    ...
"""
def read_core_db_output(dbname):
    return
