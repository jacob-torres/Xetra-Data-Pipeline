"""Xetra ETL component"""

from datetime import datetime, timedelta
from logging import getLogger
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

    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change in prev closing price
    trg_key: basic key prefix of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """

    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL():
    """    Reads the Xetra data from the Deutsche Boerse S3 bucket,
        makes transformations, and loads the new data into a target bucket.
    """

    def __init__(self, src_bucket: S3BucketConnector,
            trg_bucket: S3BucketConnector, meta_key: str,
            src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        """Constructor for Xetra ETL.

        parameters
        ----------
        src_bucket : S3BucketConnector
        Connection to the source S3 bucket

        trg_bucket : S3BucketConnector
        Connection to the target S3 bucket

        meta_key : str
        Key for meta file

        src_args : XetraSourceConfig
        NamedTuple class with source configuration data

        trg_args : XetraTargetConfig
        NamedTuple class with target configuration data
        """

        self._logger = getLogger(__name__)
        self.src_bucket = src_bucket
        self.trg_bucket = trg_bucket
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date = ''
        self.extract_date_list = []
        self.meta_update_list = []

    def extract(self):
        """Extracts data from the Deutsche Boerse S3 bucket.

        This method is used to retrieve the CSV data in the
        source S3 bucket which corresponds to the given dates,
        and stores it into a Pandas Dataframe for transformation.

        returns
        -------
        data_frame : DataFrame
        A Pandas dataframe of the extracted data
        """

        self._logger.info("Extracting the source files ...")

        # Uses the list_files_by_prefix method to get all
        # CSV files loaded to the bucket since the specified date
        files = [key for date in self.extract_date_list
            for key in self.src_bucket.list_files_by_prefix(date)]

        # Check for empty file list
        if not files:
            data_frame = DataFrame()
            self._logger.info("No files were extracted.")
            return data_frame

        data_frame = concat(
            [self.src_bucket.read_csv_to_df(file)
            for file in files], ignore_index=True
        )

        self._logger.info("Finished extracting the source files.")
        return data_frame

    def transform(self, data_frame: DataFrame):
        """Transforms the Xetra data into a form suitable for reporting.
        
        This method performs transformations on the extracted data,
        and reshapes the dataframe to report on the following:
        opening price, closing price, min and max price, daily trade volume,
        and percentage of change since last closing.

        parameters
        ----------
        data_frame : DataFrame
        A Pandas dataframe containing the extracted data

        returns
        -------
        data_frame : DataFrame
        a Pandas dataframe containing transformed report data
        """

        # Check for empty dataframe
        if data_frame.empty:
            self._logger.info("The dataframe is empty. No transformations to apply.")
            return data_frame

        self._logger.info("Transforming the Xetra data ...")

        # Select specific columns and drop all null values
        data_frame = data_frame.loc[:, self.src_args.src_columns]
        data_frame.dropna(inplace=True)

        # The opening_price column is created by sorting the data by Time,
        # then grouping it by ISIN and Date, selecting StartPrice,
        # and finally transforming the column to contain only the first date
        data_frame[self.trg_args.trg_col_op_price] = (
            data_frame.sort_values(
                by=[self.src_args.src_col_time]
            ).groupby([
                self.src_args.src_col_isin,
                self.src_args.src_col_date
            ])[self.src_args.src_col_start_price]
            .transform('first')
        )

        # The closing_price column is created by transforming the data
        # similarly to opening_price, but selecting for the last date instead
        data_frame[self.trg_args.trg_col_clos_price] = (
            data_frame.sort_values(
            by=[self.src_args.src_col_time]
            ).groupby([
                self.src_args.src_col_isin,
                self.src_args.src_col_date
            ])[self.src_args.src_col_start_price]
            .transform('last')
        )

        # Rename columns
        data_frame.rename(columns={
            self.src_args.src_col_isin: self.trg_args.trg_col_isin,
            self.src_args.src_col_date: self.trg_args.trg_col_date,
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_dail_trad_vol
        }, inplace=True)

        # Data aggregation
        data_frame = (
            data_frame.groupby([
                self.trg_args.trg_col_isin,
                self.trg_args.trg_col_date
            ], as_index=False)
            .agg({
                self.trg_args.trg_col_op_price: 'min',
                self.trg_args.trg_col_clos_price: 'min',
                self.trg_args.trg_col_min_price: 'min',
                self.trg_args.trg_col_max_price: 'max',
                self.trg_args.trg_col_dail_trad_vol: 'sum'
        })
        )

        # The prev_closing_price column is created
        # by sorting the data by Date, and then grouping it by ISIN
        # and selecting for opening price of the previous date
        data_frame[self.trg_args.trg_col_ch_prev_clos] = (
            data_frame.sort_values(by=[self.trg_args.trg_col_date])
            .groupby([self.trg_args.trg_col_isin])
            [self.trg_args.trg_col_op_price].shift(1)
        )

        # Calculate the percentage of change in the closing price since the last date
        data_frame[self.trg_args.trg_col_ch_prev_clos] = (
            (data_frame[self.trg_args.trg_col_op_price] -
                data_frame[self.trg_args.trg_col_ch_prev_clos]
            ) / data_frame[self.trg_args.trg_col_ch_prev_clos] * 100
        )

        # Round all float values to 2 decimals
        data_frame = data_frame.round(decimals=2)

        # Filter the dataframe by date
        data_frame = data_frame[
            data_frame.date >= self.extract_date
        ].reset_index(drop=True)

        self._logger.info("Finished transforming the Xetra data.")

        return data_frame

    def load(self, data_frame: DataFrame):
        """Loads the data into a new S3 bucket for reporting.

        parameters
        ----------
        data_frame : DataFrame
        A Pandas dataframe of transformed data

        returns
        -------
        bool : True if the write was successful, False if not
        """

        key_date = (
            datetime.today().date()
            .strftime(self.trg_args.trg_key_date_format)
        )

        # Format object key
        target_key = (
            self.trg_args.trg_key +
            f"_{key_date}." + self.trg_args.trg_format
        )

        new_object = self.trg_bucket.write_df_to_s3(
            target_key, data_frame, format=self.trg_args.trg_format
        )

        if new_object is None:
            self._logger.error(
                "Error: Something went wrong loading the report data."
            )
            return False

        self._logger.info("Finished loading the Xetra report.")

        # Update the meta file
        MetaProcess.update_meta_file(
            self.trg_bucket, self.meta_update_list, self.meta_key
        )

        self._logger.info("Finished updating the meta file.")
        return True


    def report(self):
        """Processes Xetra source data through ETL into a report.

        returns
        -------
        bool : is_successful
        True if the job was successful, false if not
        """

        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        is_successful = self.load(data_frame)

        if not is_successful:
            self._logger.error("Failed to create Xetra daily report.")
            return is_successful

        self._logger.info("Successfully created the Xetra daily report!")
        return is_successful