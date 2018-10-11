"""
Stores all of the ProcManagers for parallelization
"""

from .core import _empty_func, ProcManager
from .producer import Producer, ProducerManager
from .dask import DaskManager

__all__ = ['ProcManager', 'ProducerManager', 'Producer', 'DaskManager']
