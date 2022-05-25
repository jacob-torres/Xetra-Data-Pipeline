"""Test S3BucketConnector methods."""

import os
import unittest
import sys
from io import StringIO, BytesIO

import boto3
import pandas as pd
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector
from xetra.common.custom_exceptions import WrongFormatException


class TestS3BucketConnectorMethods(unittest.TestCase):
    """Testing the S3BucketConnector class."""

    def setUp(self):
        """Setting up the environment."""

        # Mock S3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Define class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-west-2.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        # Create access keys as new environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        # Create test bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)

        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
            }
        )

        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        # Create new s3 bucket connector for the test bucket
        self.s3_bucket_conn = S3BucketConnector(
                bucket_name=self.s3_bucket_name,
                access_key=self.s3_access_key,
                secret_key=self.s3_secret_key,
                endpoint_url=self.s3_endpoint_url
        )

    def tearDown(self):
        """Execute tear down after unit tests."""

        # Mocking s3 connection stop
        self.mock_s3.stop()

    def test_list_files_by_prefix_ok(self):
        """Test the list_files_by_prefix method
        by getting 2 file keys, in the case of valid prefix."""

        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f"{prefix_exp}test1.csv"
        key2_exp = f"{prefix_exp}test2.csv"

        # Test init
        csv_content = """col1,col2
        valA,valB"""

        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        # Method execution
        list_result = self.s3_bucket_conn.list_files_by_prefix(prefix_exp)

        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        # Clean up
        self.s3_bucket.delete_objects(
            Delete={
                        'Objects': [
                            {
                                'Key': key1_exp
                            },
                            {
                                'Key': key2_exp
                            }
                        ]
            }
        )

    def test_list_files_by_prefix_wrong(self):
        """Test the list_files_by_prefix method
        in case of wrong or non-existant prefix."""

        # Expected results
        prefix_exp = 'no-prefix/'

        # Method execution
        list_result = self.s3_bucket_conn.list_files_by_prefix(prefix_exp)

        # Test after method execution
        self.assertTrue(not list_result)

    def test_read_csv_to_df_ok(self):
        """Test the read_csv_to_df method for
        reading 1 csv file from the mock s3 bucket."""

        # Expected results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        log_exp = (
            f"Reading {self.s3_endpoint_url}"
            f"/{self.s3_bucket_name}"
            f"/{key_exp} ..."
        )

        # Init test
        csv_content = """col1,col2
        1,2"""

        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)

        # Method execution
        with self.assertLogs() as log:
            df_result = self.s3_bucket_conn.read_csv_to_df(key=key_exp)

            # Test log after method execution
            self.assertIn(log_exp, log.output[0])

        # Test after method execution
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertIn(col1_exp, df_result.columns)
        self.assertIn(col2_exp, df_result.columns)
        self.assertEqual(df_result[col1_exp][0], 1)
        self.assertEqual(df_result[col2_exp][0], 2)

        # Clean up
        self.s3_bucket.delete_objects(
            Delete={
                        'Objects': [
                            {
                                'Key': key_exp
                            }
                        ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """Test the write_df_to_s3 method
        in the case of n empty dataframe."""

        # Expected results
        result_exp = False
        log_exp = "The data frame is empty! No files will be written."

        # Test init
        df_empty = pd.DataFrame()
        key_exp = 'test.csv'
        file_format = 'csv'

        # Method execution
        with self.assertLogs() as log:
            result = self.s3_bucket_conn.write_df_to_s3(
                key_exp, df_empty, file_format
            )

        # Log test after method execution
        self.assertIn(log_exp, log.output[0])

        # Test after method execution
        self.assertEqual(result_exp, result)

    def test_write_df_to_s3_csv(self):
        """Test the write_df_to_s3 method
        in the case of a csv file."""

        # Expected results
        result_exp = True
        key_exp = 'test.csv'
        file_format = 'csv'
        df_exp = pd.DataFrame(
            data=[
                [1, 2],
                [3, 4]
            ],
            columns=['col1', 'col2']
        )
        log_exp = [
            (f"Preparing to write {self.s3_endpoint_url}"
            f"/{self.s3_bucket_name}/{key_exp} ..."),
            (f"Finished writing {self.s3_endpoint_url}"
            f"/{self.s3_bucket_name}/{key_exp}.")
        ]

        # Method execution
        with self.assertLogs() as log:
            result = self.s3_bucket_conn.write_df_to_s3(
                key_exp, df_exp, file_format
            )

        # Test log after method execution
        self.assertIn(log_exp[0], log.output[0])
        self.assertIn(log_exp[1], log.output[1])

        # Test after method execution
        data = (
            self.s3_bucket.Object(key=key_exp).get()
            .get('Body').read().decode('utf-8')
        )
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)

        self.assertTrue(df_exp.equals(df_result))
        self.assertEqual(result_exp, result)

        # Clean up
        self.s3_bucket.delete_objects(
            Delete={
                        'Objects': [
                            {
                                'Key': key_exp
                            }
                        ]
            }
        )

    def test_write_df_to_s3_parquet(self):
        """Test the write_df_to_s3 method
        in the case of a parquet file."""

        # Expected results
        result_exp = True
        key_exp = 'test.parquet'
        file_format = 'parquet'
        df_exp = pd.DataFrame(
            data=[
                [1, 2],
                [3, 4]
            ],
            columns=['col1', 'col2']
        )
        log_exp = [
            (f"Preparing to write {self.s3_endpoint_url}"
            f"/{self.s3_bucket_name}/{key_exp} ..."),
            (f"Finished writing {self.s3_endpoint_url}"
            f"/{self.s3_bucket_name}/{key_exp}.")
        ]

        # Method execution
        with self.assertLogs() as log:
            result = self.s3_bucket_conn.write_df_to_s3(
                key_exp, df_exp, file_format
            )

        # Test log after method execution
        self.assertIn(log_exp[0], log.output[0])
        self.assertIn(log_exp[1], log.output[1])

        # Test after method execution
        data = (
            self.s3_bucket.Object(key=key_exp).get()
            .get('Body').read()
        )
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)

        self.assertTrue(df_exp.equals(df_result))
        self.assertEqual(result_exp, result)

        # Clean up
        self.s3_bucket.delete_objects(
            Delete={
                        'Objects': [
                            {
                                'Key': key_exp
                            }
                        ]
            }
        )

    def test_write_df_to_s3_wrong_format(self):
        """Test the write_df_to_s3 method
        in the case of a file with an invalid format."""

        # Expected results
        result_exp = False
        key_exp = 'test.parquet'
        file_format = 'wrongformat'
        df_exp = pd.DataFrame(
            data=[
                [1, 2],
                [3, 4]
            ],
            columns=['col1', 'col2']
        )
        log_exp = (
            f"Error: {file_format} is not a valid file type."
            " No files will be written."
        )
        exception_exp = WrongFormatException

        # Method execution
        with self.assertLogs() as log:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(
                    key_exp, df_exp, file_format
                )

            # Test log after method execution
            self.assertIn(log_exp, log.output[1])


if __name__ == '__main__':
    unittest.main()
