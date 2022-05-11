"""Xetra ETL component"""
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector

class XetraSourceConfig(NamedTuple):
    """Class for source configuration data.

    src_first_extract_date: Determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source
    """

    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """Class for target configuration data.

    tgt_col_isin: column name for isin in target
    tgt_col_date: column name for date in target
    tgt_col_op_price: column name for opening price in target
    tgt_col_clos_price: column name for closing price in target
    tgt_col_min_price: column name for minimum price in target
    tgt_col_max_price: column name for maximum price in target
    tgt_col_dail_trad_vol: column name for daily traded volume in target
    tgt_col_ch_prev_clos: column name for change to previous day's closing price in target
    tgt_key: basic key of target file
    tgt_key_date_format: date format of target file key
    tgt_format: file format of the target file
    """

    tgt_col_isin: str
    tgt_col_date: str
    tgt_col_op_price: str
    tgt_col_clos_price: str
    tgt_col_min_price: str
    tgt_col_max_price: str
    tgt_col_dail_trad_vol: str
    tgt_col_ch_prev_clos: str
    tgt_key: str
    tgt_key_date_format: str
    tgt_format: str


class XetraETL():
"""    Reads the Xetra data from the Deutsche Boerse S3 bucket,
    makes transformations, and loads the new data into a target bucket.
"""

    def __init__(self, src_bucket: S3BucketConnector,
            tgt_bucket: S3BucketConnector, meta_key: str,
            src_args: XetraSourceConfig, tgt_args: XetraTargetConfig):
        """Constructor for Xetra transformer.

        parameters
        ----------
        src_bucket : S3BucketConnector
        Connection to the source S3 bucket

        tgt_bucket : S3BucketConnector
        Connection to the target S3 bucket

        meta_key : str
        Key for meta file

        src_args : XetraSourceConfig
        NamedTuple class with source configuration data

        tgt_args : XetraTargetConfig
        NamedTuple class with target configuration data
        """

        self.src_bucket = src_bucket
        self.tgt_bucket = tgt_bucket
        self.meta_key = meta_key
        self.src_args = src_args
        self.tgt_args = tgt_args
        self.extract_date = None
        self.extract_date_list = None
        self.meta_update_list = None


    def extract(self):
        pass


    def transform(self):
        pass


    def load(self):
        pass


    def etl_report(self):
        pass
