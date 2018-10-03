"""
Different optimization functions that can be used to search through parameters.
These functions act like scipy.optimize.minimize and output OptimizeResult
objects.

Currently only includes SPSA
"""

from .spsa import SPSA


__all__ = ['SPSA']
