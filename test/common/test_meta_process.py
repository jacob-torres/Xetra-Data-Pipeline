"""TestMetaProcessMethods."""
import os
import unittest
from io import StringIO
from datetime import datetime, timedelta

import boto3
import pandas as pd
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException


class TestMetaProcessMethods(unittest.TestCase):
    """Testing the MetaProcess class."""

    def setUp(self):
        """Set up the test environment."""

        # mock s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Define the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-west-2.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        # Create s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        # Create a bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
        }
        )
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        # Create a S3BucketConnector instance
        self.s3_bucket_meta = S3BucketConnector(
            self.s3_bucket_name,
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url
        )
        self.dates = [
            (datetime.today().date() - timedelta(days=day))
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            for day in range(8)
        ]

    def tearDown(self):
        """Clean up the test environment."""

        # Mock s3 connection stop
        self.mock_s3.stop()

    def test_update_meta_file_no_meta_file(self):
        """Test the update_meta_file method
        when there is no meta file."""

        # Expected results
        date_list_exp = ['2022-05-25', '2022-05-26']
        proc_date_list_exp = [datetime.today().date()] * 2

        # Test init
        meta_key = 'meta.csv'

        # Method execution
        MetaProcess.update_meta_file(
            self.s3_bucket_meta, date_list_exp, meta_key
        )

        # Read meta file
        data = (
            self.s3_bucket.Object(key=meta_key).get()
            .get('Body').read().decode('utf-8')
        )
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(
            df_meta_result[
                MetaProcessFormat.META_SOURCE_DATE_COL.value
            ]
        )
        proc_date_list_result = list(
            pd.to_datetime(
                df_meta_result[
                    MetaProcessFormat.META_PROCESS_COL.value
                ]
            ).dt.date
        )

        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)

        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_empty_date_list(self):
        """Test the update_meta_file method
        with an empty extract_date_list argument."""

        # Expected results
        result_exp = False
        log_exp = "The data frame is empty! No files will be written."

        # Test init
        date_list = []
        meta_key = 'meta.csv'

        # Method execution
        with self.assertLogs() as log:
            result = MetaProcess.update_meta_file(
                self.s3_bucket_meta, date_list, meta_key
            )

            # Log test after method execution
            self.assertIn(log_exp, log.output[1])

        # Test after method execution
        self.assertEqual(result_exp, result)

    def test_update_meta_file_meta_file_ok(self):
        """Tests the update_meta_file method
        when there is already a meta file."""

        # Expected results
        date_list_old = ['2022-05-12', '2022-05-13']
        date_list_new = ['2022-05-01', '2022-05-02']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},"
          f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
          f"{date_list_old[0]},"
          f"{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n"
          f"{date_list_old[1]},"
          f"{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)

        # Method execution
        MetaProcess.update_meta_file(
            self.s3_bucket_meta, date_list_new, meta_key
        )

        # Read meta file
        data = (
            self.s3_bucket.Object(key=meta_key).get()
            .get('Body').read().decode('utf-8')
        )
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(
            df_meta_result[
                MetaProcessFormat.META_SOURCE_DATE_COL.value
            ]
        )
        proc_date_list_result = list(
            pd.to_datetime(
                df_meta_result[
                    MetaProcessFormat.META_PROCESS_COL.value
                ]
            )
                .dt.date)

        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)

        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_meta_file_wrong(self):
        """Tests the update_meta_file method
        when there is a wrong meta file."""

        # Expected results
        date_list_old = ['2022-04-20', '2022-04-21']
        date_list_new = ['2022-04-26', '2022-04-27']

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f"wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n"
          f"{date_list_old[0]},"
          f"{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n"
          f"{date_list_old[1]},"
          f"{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)

        # Method execution
        with self.assertRaises(WrongMetaFileException):
            MetaProcess.update_meta_file(
                self.s3_bucket_meta, date_list_new, meta_key
            )

        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_get_date_list_no_meta_file(self):
        """Tests the get_date_list method
        when there is no meta file."""

        # Expected results
        date_list_exp = [
            (datetime.today().date() - timedelta(days=x))
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for x in range(4)
            ]
        min_date_exp = (
            (datetime.today().date() - timedelta(days=3))
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        )

        # Test init
        first_date = min_date_exp
        meta_key = 'meta.csv'

        # Method execution
        min_date_result, date_list_result = MetaProcess.get_date_list(
            self.s3_bucket_meta, first_date, meta_key
            )

        # Test after method execution
        self.assertEqual(set(date_list_exp), set(date_list_result))
        self.assertEqual(min_date_exp, min_date_result)

    def test_get_date_list_meta_file_ok(self):
        """Tests the get_date_list method
        when there is a meta file"""

        # Expected results
        min_date_exp = [
            (datetime.today().date() - timedelta(days=1))
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (datetime.today().date() - timedelta(days=2))
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (datetime.today().date() - timedelta(days=7))
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        ]

        date_list_exp = [
            [
                (datetime.today().date() - timedelta(days=day))
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(3)
            ],
            [
                (datetime.today().date() - timedelta(days=day))
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(4)
            ],
            [
                (datetime.today().date() - timedelta(days=day))
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                for day in range(9)
            ]
        ]

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},"
          f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
          f"{self.dates[3]},{self.dates[0]}\n"
          f"{self.dates[4]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date_list = [
          self.dates[1],
          self.dates[4],
          self.dates[7]
        ]

        # Method execution
        for count, first_date in enumerate(first_date_list):
            min_date_result, date_list_result = MetaProcess.get_date_list(
                self.s3_bucket_meta, first_date, meta_key
            )

            # Test after method execution
            self.assertEqual(set(date_list_exp[count]), set(date_list_result))
            self.assertEqual(min_date_exp[count], min_date_result)

        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_get_date_list_meta_file_wrong(self):
        """Tests the get_date_list method
        when there is a wrong meta file."""

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f"wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n"
          f"{self.dates[3]},{self.dates[0]}\n"
          f"{self.dates[4]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[1]

        # Method execution
        with self.assertRaises(KeyError):
            MetaProcess.get_date_list(
                self.s3_bucket_meta, first_date, meta_key
            )

        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_get_date_list_empty_date_list(self):
        """Tests the get_date_list method
        when there are no dates to be returned."""

        # Expected results
        min_date_exp = '2500-01-01'
        date_list_exp = []

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},"
          f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
          f"{self.dates[0]},{self.dates[0]}\n"
          f"'{self.dates[1]},{self.dates[0]}'"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[0]

        # Method execution
        min_date_result, date_list_result = MetaProcess.get_date_list(
            self.s3_bucket_meta, first_date, meta_key
        )

        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(min_date_exp, min_date_result)

        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )


if __name__ == '__main__':
    unittest.main()
