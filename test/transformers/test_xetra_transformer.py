"""Test XetraETL Methods."""
import os
import unittest
from unittest.mock import patch
from io import BytesIO

import boto3
import pandas as pd
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


class TestXetraETLMethods(unittest.TestCase):
    """Test the XetraETL class."""

    def setUp(self):
        """Set up the test environment."""

        # mock s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Define the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-west-2.amazonaws.com'
        self.s3_bucket_name_src = 'src-bucket'
        self.s3_bucket_name_trg = 'trg-bucket'
        self.meta_key = 'meta_key'

        # Create s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        # Create the source and target bucket on the mocked s3
        self.s3 = boto3.resource(
            service_name='s3',
            endpoint_url=self.s3_endpoint_url
        )
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name_src,
            CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
            }
        )
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name_trg,
            CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
            }
        )

        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)

        # Create S3BucketConnector testing instances
        self.s3_bucket_src = S3BucketConnector(
            self.s3_bucket_name_src,
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url
        )
        self.s3_bucket_trg = S3BucketConnector(
            self.s3_bucket_name_trg,
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url
        )

        # Create source and target configuration
        conf_dict_src = {
            'src_first_extract_date': '2021-04-01',
            'src_columns': [
                'ISIN', 'Mnemonic', 'Date', 'Time',
            'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'
        ],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }
        conf_dict_trg = {
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_dail_trad_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report/xetra_daily_report',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }
        self.source_config = XetraSourceConfig(**conf_dict_src)
        self.target_config = XetraTargetConfig(**conf_dict_trg)

        # Creating source files on mocked s3
        columns_src = [
            'ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice',
            'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'
        ]

        data = [
            ['AT0000A0E9W5', 'SANT', '2021-04-15', '12:00', 20.19, 18.45, 18.20, 20.33, 877],
            ['AT0000A0E9W5', 'SANT', '2021-04-16', '15:00', 18.27, 21.19, 18.27, 21.34, 987],
            ['AT0000A0E9W5', 'SANT', '2021-04-17', '13:00', 20.21, 18.27, 18.21, 20.42, 633],
            ['AT0000A0E9W5', 'SANT', '2021-04-17', '14:00', 18.27, 21.19, 18.27, 21.34, 455],
            ['AT0000A0E9W5', 'SANT', '2021-04-18', '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
            ['AT0000A0E9W5', 'SANT', '2021-04-18', '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
            ['AT0000A0E9W5', 'SANT', '2021-04-19', '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
            ['AT0000A0E9W5', 'SANT', '2021-04-19', '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
            ['AT0000A0E9W5', 'SANT', '2021-04-19', '09:00', 24.22, 22.21, 22.21, 25.01, 1523]
        ]

        self.df_src = pd.DataFrame(data, columns=columns_src)
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-15/2021-04-15_BINS_XETR12.csv',
                        self.df_src.loc[0:0], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-16/2021-04-16_BINS_XETR15.csv',
            self.df_src.loc[1:1], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-17/2021-04-17_BINS_XETR13.csv',
            self.df_src.loc[2:2], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-17/2021-04-17_BINS_XETR14.csv',
                        self.df_src.loc[3:3], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-18/2021-04-18_BINS_XETR07.csv',
                        self.df_src.loc[4:4], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-18/2021-04-18_BINS_XETR08.csv',
                        self.df_src.loc[5:5], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-19/2021-04-19_BINS_XETR07.csv',
                        self.df_src.loc[6:6], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-19/2021-04-19_BINS_XETR08.csv',
                        self.df_src.loc[7:7], 'csv'
        )
        self.s3_bucket_src.write_df_to_s3(
            '2021-04-19/2021-04-19_BINS_XETR09.csv',
                        self.df_src.loc[8:8], 'csv'
        )

        columns_report = [
            'ISIN', 'date', 'opening_price_eur', 'closing_price_eur',
            'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%'
        ]

        data_report = [
            ['AT0000A0E9W5', '2021-04-17', 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
            ['AT0000A0E9W5', '2021-04-18', 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
            ['AT0000A0E9W5', '2021-04-19', 23.58, 24.22, 22.21, 25.01, 3586, 14.58]
        ]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        # mock s3 connection stop
        self.mock_s3.stop()

    def test_extract_no_files(self):
        """Tests the extract method when
        there are no files to be extracted."""

        # Test init
        extract_date = '2500-01-02'
        extract_date_list = []

        # Method execution
        with patch.object(MetaProcess, "get_date_list",
                return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(
                self.s3_bucket_src, self.s3_bucket_trg, self.meta_key,
                self.source_config, self.target_config
            )
            df_result = xetra_etl.extract()

        # Test after method execution
        self.assertTrue(df_result.empty)

    def test_extract_files(self):
        """Tests the extract method
        when there are files to be extracted."""

        # Expected results
        df_exp = self.df_src.loc[1:8].reset_index(drop=True)

        # Test init
        extract_date = '2021-04-17'
        extract_date_list = [
            '2021-04-16', '2021-04-17', '2021-04-18',
            '2021-04-19', '2021-04-20'
        ]

        # Method execution
        with patch.object(MetaProcess, "get_date_list",
                return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(
                self.s3_bucket_src, self.s3_bucket_trg, self.meta_key,
                self.source_config, self.target_config
            )
            xetra_etl.extract_date = extract_date
            xetra_etl.extract_date_list = extract_date_list
            df_result = xetra_etl.extract()

        # Test after method execution
        self.assertTrue(df_exp.equals(df_result))

    def test_transform_emptydf(self):
        """Tests the transform method with
        an empty DataFrame as input."""

        # Expected results
        log_exp = "The dataframe is empty. No transformations to apply."

        # Test init
        extract_date = '2021-04-17'
        extract_date_list = [
            '2021-04-16', '2021-04-17', '2021-04-18'
        ]
        df_input = pd.DataFrame()

        # Method execution
        with patch.object(MetaProcess, "get_date_list",
                return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(
                self.s3_bucket_src, self.s3_bucket_trg, self.meta_key,
                self.source_config, self.target_config
            )

            with self.assertLogs() as log:
                df_result = xetra_etl.transform(df_input)

                # Log test after method execution
                self.assertIn(log_exp, log.output[0])

        # Test after method execution
        self.assertTrue(df_result.empty)

    def test_transform_ok(self):
        """Tests the transform method with
        a DataFrame as input."""

        # Expected results
        log1_exp = "Transforming the Xetra data ..."
        log2_exp = "Finished transforming the Xetra data."
        df_exp = self.df_report

        # Test init
        extract_date = '2021-04-17'
        extract_date_list = [
            '2021-04-16', '2021-04-17', '2021-04-18', '2021-04-19'
        ]
        df_input = self.df_src.loc[1:8].reset_index(drop=True)

        # Method execution
        with patch.object(MetaProcess, "get_date_list",
                return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(
                self.s3_bucket_src, self.s3_bucket_trg, self.meta_key,
                self.source_config, self.target_config
            )
            #xetra_etl.extract_date = extract_date
            #xetra_etl.extract_date_list = extract_date_list

            with self.assertLogs() as log:
                df_result = xetra_etl.transform(df_input)

                # Log test after method execution
                self.assertIn(log1_exp, log.output[0])
                self.assertIn(log2_exp, log.output[1])

        # Test after method execution
        self.assertTrue(df_exp.equals(df_result))

    def test_load(self):
        """Tests the load method."""

        # Expected results
        log1_exp = "Finished loading the Xetra report."
        log2_exp = "Finished updating the meta file."
        df_exp = self.df_report
        meta_exp = [
            '2021-04-17', '2021-04-18', '2021-04-19'
        ]

        # Test init
        extract_date = '2021-04-17'
        extract_date_list = [
            '2021-04-16', '2021-04-17', '2021-04-18', '2021-04-19'
        ]
        df_input = self.df_report

        # Method execution
        with patch.object(MetaProcess, "get_date_list",
                return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(
                self.s3_bucket_src, self.s3_bucket_trg, self.meta_key,
                self.source_config, self.target_config
            )
            xetra_etl.extract_date = extract_date
            xetra_etl.extract_date_list = extract_date_list
            xetra_etl.meta_update_list = meta_exp

            with self.assertLogs() as log:
                xetra_etl.load(df_input)

                # Log test after method execution
                self.assertIn(log1_exp, log.output[1])
                self.assertIn(log2_exp, log.output[4])

        # Test after method execution
        trg_file = self.s3_bucket_trg.list_files_by_prefix(
            self.target_config.trg_key)[0]
        data = (
            self.trg_bucket.Object(key=trg_file).get()
            .get('Body').read()
        )
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))

        meta_file = self.s3_bucket_trg.list_files_by_prefix(
            self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)

        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': trg_file
                    },
                    {
                        'Key': trg_file
                    }
                ]
            }
        )

    def test_report(self):
        """Tests the report method."""

        # Expected results
        df_exp = self.df_report
        meta_exp = [
            '2021-04-17', '2021-04-18', '2021-04-19'
        ]

        # Test init
        extract_date = '2021-04-17'
        extract_date_list = [
            '2021-04-16', '2021-04-17', '2021-04-18', '2021-04-19'
        ]

        # Method execution
        with patch.object(MetaProcess, "get_date_list",
                return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(
                self.s3_bucket_src, self.s3_bucket_trg, self.meta_key,
                self.source_config, self.target_config
            )
            xetra_etl.extract_date = extract_date
            xetra_etl.extract_date_list = extract_date_list
            xetra_etl.report()

        # Test after method execution
        trg_file = self.s3_bucket_trg.list_files_by_prefix(
            self.target_config.trg_key)[0]
        data = (
            self.trg_bucket.Object(key=trg_file).get()
            .get('Body').read()
        )
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))

        meta_file = self.s3_bucket_trg.list_files_by_prefix(
            self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)

        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': trg_file
                    },
                    {
                        'Key': trg_file
                    }
                ]
            }
        )


if __name__ == '__main__':
    unittest.main()
