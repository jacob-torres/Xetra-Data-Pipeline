"""Xetra ETL component"""
from datetime import datetime, timedelta
from typing import NamedTuple

from pandas import DataFrame, concat

from xetra.common.meta_process import MetaProcess
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

    def extract(self, bucket: S3BucketConnector, date_list: list):
        """Extracts data from the Deutsche Boerse S3 bucket.

        This method is used to retrieve the CSV data in the
        S3 bucket which corresponds to the given dates, and stores it
        into a Pandas Dataframe for transformation.

        parameters
        ----------
        bucket : S3BucketConnector
        The source S3 bucket from which to extract data

        date_list : list
        The list of dates from which to extract data

        returns
        -------
        df : DataFrame
        A Pandas dataframe of the extracted data
        """

        # Uses the list_files_in_prefix method to get all CSV files 
    # loaded to the bucket since the specified date.
        files = [key for date in date_list for key in bucket.list_files_in_prefix(bucket, date)]

        # Todo: Add an exception for empty file list
        df = concat([bucket.read_csv_to_df(bucket, obj) for obj in files], ignore_index=True)

        return df


    def transform(self, df: DataFrame, columns: list, arg_date: str):
        """Transforms the Xetra data into a form suitable for reporting.
        
        This method performs transformations on the extracted data,
        and reshapes the dataframe to report on facts such as
        opening price, closing price, min and max price,
        percentage of change since last closing, ETC.

        parameters
        ----------
        df : DataFrame
        A dataframe of extracted data to transform

        columns : list
        A list of column names to be included in the report

        arg_date : str
        A date (in YYYY-MM-DD format) to filter the data

        returns
        -------
        df : DataFrame
        a transformed dataframe for loading
        """

        df = df.loc[:, columns]
        df.dropna(inplace=True)

        # The opening_price column is created by sorting the data by Time,
        # then grouping it by ISIN and Date, selecting StartPrice,
        # and finally transforming the column to contain only the first date
        df['opening_price'] = df.sort_values(
            by=['Time']).groupby(
            ['ISIN', 'Date'])['StartPrice'].transform('first')

        # The closing_price column is created by transforming the data
        # similarly to opening_price, but selecting for the last date instead
        df['closing_price'] = df.sort_values(
            by=['Time']).groupby(
            ['ISIN', 'Date'])['StartPrice'].transform('last')

        # The dataframe is grouped by ISIN and Date, then aggregated to create
        # new columns to express opening, closing, min, max, and trade volume
        df = df.groupby(['ISIN', 'Date'], as_index=False).agg(
            opening_price_eur=('opening_price', 'min'),
            closing_price_eur=('closing_price', 'min'),
            minimum_price_eur=('MinPrice', 'min'),
            maximum_price_eur=('MaxPrice', 'max'),
            daily_traded_volume=('TradedVolume', 'sum'))

        # The prev_closing_price column is created
        # by sorting the data by Date, and then grouping it by ISIN
        # and selecting for closing_price_eur of the previous date
        df['prev_closing_price'] = df.sort_values(
            by=['Date']).groupby(
            ['ISIN'])['closing_price_eur'].shift(1)

        # The change_prev_closing_percent column is created
        # by subtracting the current and prev closing prices
        # and dividing the result by the prev price times 100. This results in
        # the percentage of change in the closing price since the last date
        df['change_prev_closing_%'] = (
            df['closing_price_eur'] - df['prev_closing_price']
            ) / df['prev_closing_price'] * 100

        df.drop(columns=['prev_closing_price'], inplace=True)
        df = df.round(decimals=2)
        df = df[df.Date >= arg_date]

        return df


    def load(self, bucket: S3BucketConnector,
    df: DataFrame, tgt_key: str, tgt_format: str,
        meta_key: str, extract_date_list: list):
        """Loads the data into a new S3 bucket for reporting.
        
        This method is used to load the newly transformed data
        into a target S3 bucket as an Apache parquet object
        as a daily report.

        parameters
        ----------
        bucket : S3BucketConnector
        The target S3 bucket to which to load the report

        df : DataFrame
        A Pandas dataframe of transformed report data

        tgt_key : str
        The target key specified for the current report

        tgt_format : str
        The file format of the target report

        meta_key : str
        The key of the meta file

        extract_date_list : list
        A list of extraction dates to note in the meta file

        returns
        -------
        bool : is_successful
        True if the write was successful, False if not
        """

        key_date_format = '%Y%m%d_%H%M%S'
        key = tgt_key + datetime.today().strftime(key_date_format) + tgt_format

        if len(df) < 1:
            print("Sorry, no data was extracted. Try another date.")
            is_successful = False

        MetaProcess.write_df_to_s3(bucket, key, df, format=tgt_format)
        MetaProcess.update_meta_file(bucket, meta_key, extract_date_list)
        is_successful = True

        return is_successful


    def report(self, src_bucket: S3BucketConnector,
        tgt_bucket: S3BucketConnector, date_list: list, columns: list,
        arg_date: str, tgt_key: str, tgt_format: str, meta_key: str):
        """Processes Xetra source data through ETL into a report.
        
        This method uses ETL to extract, transform,
        and load the source data into a report.

        parameters
        ----------
        src_bucket : S3BucketConnector
        The source S3 bucket from which to extract the data

        tgt_bucket : S3BucketConnector
        The target S3 bucket to which to load the report

        date_list : list
        A list of dates from which to filter the data

        columns : list
        A list of column names to include in the report

        arg_date : str
        A date (in YYYY-MM-DD format) for which to extract data

        tgt_key : str
        The target key for the new report

        tgt_format : str
        The file format for the new report

        meta_key : str
        The key for the meta file

        returns
        -------
        bool : is_successful
        True if the load was successful, False if not
        """

        df = self.extract(src_bucket, date_list)
        df = self.transform(df, columns, arg_date)
        extract_date_list = [date for date in date_list if date >= arg_date]

        # Check the load success
        is_successful = self.load(
            tgt_bucket, df, tgt_key, tgt_format, meta_key, extract_date_list
        )

        return is_successful
