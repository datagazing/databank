"""
Class providing convenient access to data sets

Features
--------

* Provides an abstraction for data sets
    * Uses pandas.DataFrame objects internally
* Optional automatic data set indexing and cataloging
    * Makes finding data sets that measure the same variable easy
"""

__author__ = """Brendan Strejcek"""
__email__ = 'brendan@datagazing.com'
__version__ = '0.1.0'

from .databank import Data # noqa F401
