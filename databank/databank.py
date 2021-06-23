#!/usr/bin/env python3

"""
See top level package docstring for documentation
"""

import copy
import logging
import pathlib
import re

import attr
import pandas
import pyreadstat

import disambigufile
import pandect
import optini

myself = pathlib.Path(__file__).stem

# configure library-specific logger
logger = logging.getLogger(myself)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(name)s: %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

########################################################################


@attr.s(auto_attribs=True)
class Data:
    """
    Class providing access to a data set

    - Much of this class wraps pandas.DataFrame objects
    - Conceptually this might make sense as subclass of pandas.DataFrame
        - Pandas internals are too complicated
        - More reliable to provide select wrapper methods

    Attributes
    ----------

    source : str, default=None
        yyy
    """
    source: str = None
    datapath: str = ''
    subpattern: str = r'^data'
    _data: pandas.DataFrame = None
    _data_orig: pandas.DataFrame = None
    _meta: pyreadstat.metadata_container = None
    _meta_orig: pyreadstat.metadata_container = None

    def __attrs_post_init__(self):
        """Constructor"""
        self.use(source=self.source)

    def use(self, source=None, clear=False):
        """Locate and load data set"""

        if source is None:
            logger.debug('source unspecified')
            return

        if type(self._data) == pandas.DataFrame and not clear:
            logger.warning('cowardly refusing to replace active data set')
            logger.warning('set clear=True if you are sure')
            return

        if 'Datapath' in optini.opt:
            option_datapath = optini.opt.Datapath
            logger.debug(f"adding datapath from options: {option_datapath}")
            # self.datapath = ':'.join(option_datapath.split(':'))
            newpath = self.datapath.split(':') + option_datapath.split(':')
            newpath = ':'.join(newpath)
            newpath = re.sub(r'^:', '', newpath)
            self.datapath = newpath
        try:
            logger.debug(f"datapath = {self.datapath}")
            hit = disambigufile.DisFile(
                pattern=source,
                subpattern=self.subpattern,
                path=self.datapath,
            )
            logger.debug(f"found data set: {hit}")
        except disambigufile.AmbiguousMatchError as e:
            logger.warning(f"unable to find data set: {source}")
            logger.warning(f"ambiguous source")
            logger.debug(f"exception details: {e}")
            return
        except disambigufile.NoMatchError as e:
            logger.warning(f"unable to find data set: {source}")
            logger.warning(f"no match")
            logger.debug(f"exception details: {e}")
            return
        try:
            self._data_orig, self._meta_orig = pandect.load(str(hit))
            logger.info(f"loaded data from: {hit}")
        except pandect.Error as e:
            logger.warning(f"unable to load data file: {hit}")
            logger.debug(f"exception details: {e}")
            return
     
        self._data = copy.deepcopy(self._data_orig)
        self._meta = copy.deepcopy(self._meta_orig)

    def stack(self):
        """yyy Add new rows from another dataset"""
        pass

    def sortvars(self):
        vars = sorted(list(self._data))
        self._data = self._data[vars]
        logger.info('sorted order of variables')

    def save(self,
        output,
        missing='',
        version=None,
        index=None,
        sortvar=False,
    ):
        # yyy not used: missing version index sort
        pandect.save(data=self._data, output=output, meta=self._meta)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        # yyy write better representation
        return str(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def eval(self, expr, **kwargs):
        """Apply Pandas eval expression to data set"""
        self._data = self._data.eval(expr, **kwargs)

    def query(self, expr, **kwargs):
        """Apply Pandas query expression to data set"""
        self._data = self._data.query(expr, **kwargs)
 

if __name__ == '__main__':
    pass
