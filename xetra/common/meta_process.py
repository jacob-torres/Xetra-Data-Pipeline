"""Methods for processing the meta file."""
from datetime import datetime, timedelta

from pandas import DataFrame, read_csv, concat, to_datetime

from s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL


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
    def update_meta_file(self, bucket: S3BucketConnector,
        meta_key: str, extract_date_list: list):
        """Updates the meta file with the new dates from the latest report.
        
        The meta file is updated with the date(s)
        associated with the extracted data, and the date(s)
        and time(s) of the current ETL process.

        parameters
        ----------
        bucket : S3BucketConnector
        The source S3 bucket from which the data is being extracted

        meta_key : str
        The key of the meta file object

        extract_date_list : list
        A list of dates associated with the extracted data
        """

        columns = ['source_date', 'datetime_of_processing']
        date_format = '%Y-%m-%d'
        df_new = DataFrame(columns=columns)
        df_new['source_date'] = extract_date_list
        df_new['datetime_of_processing'] = datetime.today().strftime(date_format)
        df_old = bucket.read_csv_to_df(key=meta_key)
        df_all = concat([df_old, df_new])

        bucket.write_df_to_s3(meta_key, df_all)

    @staticmethod
    def get_date_list(self, bucket: S3BucketConnector,
        arg_date: str, date_format: str, meta_key: str):
        """Returns a list of possible dates based on the argument date.
        
        This method is used to generate a list of extraction dates
        to update the meta file.

        parameters
        ----------
        bucket : S3BucketConnector
        The source S3 bucket from which to extract the dates

        arg_date : str
        A date used as the starting date to extract

        date_format : str
        The string used to format the argument date

        meta_key : str
        The key for the meta file object

        returns
        -------
        min_date_result : str
        The minimum date for extracting data from the bucket

        date_results : list
        A list of dates from the minimum date until today's date
        """

        # Set the minimum date to the previous day (in datetime format)
        min_date = datetime.strptime(arg_date, date_format).date() - timedelta(days=1)
        today = datetime.today().date()

        try:
            # Read the meta file in the target S3 bucket
            df_meta = bucket.read_csv_to_df(bucket, meta_key)

            # The date list counts up from min_date to the current date
            dates = [(min_date + timedelta(days=x)) for x in range(0, (today-min_date).days + 1)]

            # Create set of unique dates in the meta file
            # and convert them to pandas datetime objects
            src_dates = set(to_datetime(df_meta['source_date']).dt.date)

            # Finds list of dates not yet added to the meta file
            missing_dates = set(dates[1:]) - src_dates

            if missing_dates:
                # Set min_date to the day before first date not in the meta file
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                date_results = [date.strftime(date_format) for date in dates if date >= min_date]
                min_date_result = (min_date + timedelta(days=1)).strftime(date_format)

            else:
                date_results = []
                min_date_result = datetime(2500, 1, 1).date()

        except bucket.session.Session().client('s3').exceptions.NoSuchKey:
            date_results = [(min_date + timedelta(days=x)).strftime(date_format) for x in range(0, (today-min_date).days + 1)]
            min_date_result = arg_date

        return min_date_result, date_results
