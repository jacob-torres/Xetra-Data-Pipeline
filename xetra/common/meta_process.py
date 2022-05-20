"""Methods for processing the meta file."""

from collections import Counter
from datetime import datetime, timedelta
from doctest import DONT_ACCEPT_TRUE_FOR_1

from pandas import DataFrame, read_csv, concat, to_datetime

from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.s3 import S3BucketConnector


class MetaProcess():
    """Class for working with the meta file.
    
    The meta file is updated whenever a new ETL job is completed.
    It is a csv file containing two columns:
    source_date and datetime_of_processing.

    The source_date column contains the date of the original data,
    and the datetime_of_processing column contains the date and time of the
    ETL job for creating the daily report.
    """

    @staticmethod
    def update_meta_file(bucket: S3BucketConnector,
        extract_date_list: list, meta_key: str = 'meta.csv'):
        """Updates the meta file with the new dates from the latest report.
        
        The meta file is updated with the date(s)
        associated with the extracted data, and the datetime(s)
        of the current ETL process.

        parameters
        ----------
        bucket : S3BucketConnector
        The source S3 bucket from which the data is being extracted

        extract_date_list : list
        A list of dates associated with the extracted data

        meta_key : str
        The key of the meta file object (defaults to 'meta.csv')

        returns
        -------
        bool : True if writing the meta file was successful, False if not
        """

        # Define meta constants
        source_date = MetaProcessFormat.META_SOURCE_DATE_COL.value
        datetime_of_processing = MetaProcessFormat.META_PROCESS_COL.value
        date_format = MetaProcessFormat.META_DATE_FORMAT.value
        datetime_format = MetaProcessFormat.META_PROCESS_DATE_FORMAT.value

        # Create dataframe for new meta data
        df_new = DataFrame(
            columns=['source_date', 'datetime_of_processing']
        )
        df_new[source_date] = extract_date_list
        df_new[datetime_of_processing] = (
            datetime.today().strftime(datetime_format)
        )

        try:
            # Create dataframe for old meta data if it exists
            df_old = bucket.read_csv_to_df(key=meta_key)

            if Counter(df_old.columns) != Counter(df_new.columns):
                # The format of the 2 meta files are not the same
                raise WrongMetaFileException

            else:
                df_all = concat([df_old, df_new])

        except bucket.session.client('s3').exceptions.NoSuchKey:
            # If the meta file does not exist in the bucket
            df_all = df_new

        return bucket.write_df_to_s3(meta_key, df_all)

    @staticmethod
    def get_date_list(bucket: S3BucketConnector,
        start_date: str, meta_key: str = 'meta.csv'):
        """Returns a list of extraction dates based on the start date.
        The current date is used as the processing date.

        parameters
        ----------
        bucket : S3BucketConnector
        The source S3 bucket from which to extract the dates

        start_date : str
        A date used as the starting date to extract

        meta_key : str
        The key for the meta file (defaults to 'meta.csv')

        returns
        -------
        min_date_result : str
        The minimum date for extracting data from the bucket

        date_results : list
        A list of dates from the minimum date until the current date
        """

        date_format = MetaProcessFormat.META_DATE_FORMAT.value

        # Set the minimum date to the previous day (in datetime format)
        min_date = (
            datetime.strptime(start_date, date_format).date()
            - timedelta(days=1)
        )

        today = datetime.today().date()

        try:
            # Read the meta file in the S3 bucket
            df_meta = bucket.read_csv_to_df(meta_key)

            # The date list counts up from min_date to the current date
            dates = [
                (min_date + timedelta(days=x))
                for x in range(0, (today-min_date).days + 1)
            ]

            # Create set of unique dates in the meta file
            # and convert them to pandas datetime objects
            src_dates = set(
                to_datetime(df_meta['source_date']
            ).dt.date)

            # Finds list of dates not yet added to the meta file
            missing_dates = (
                set(dates[1:]) - src_dates
            )

            if missing_dates:
                # Set min_date to the day before first date not in the meta file
                min_date = (
                    min(
                        set(dates[1:]) - src_dates
                    ) - timedelta(days=1)
                )

                date_results = [
                    date.strftime(date_format)
                    for date in dates if date >= min_date
                ]

                min_date_result = (
                    (min_date + timedelta(days=1)).strftime(date_format)
                )

            else:
                date_results = []
                min_date_result = datetime(2500, 1, 1).date()

        except bucket.session.client('s3').exceptions.NoSuchKey:
            date_results = [
                (start_date+ timedelta(days=x)).strftime(date_format)
                for x in range(0, (today-min_date).days + 1)
            ]

            min_date_result = start_date

        return min_date_result, date_results
