"""Runs the Xetra ETL application."""

from logging import getLogger
from logging.config import dictConfig
from pydoc import source_synopsis

from yaml import safe_load

from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    """Entry-point for running the Xetra ETL application."""

    logger = getLogger(__name__)

    # Parse the yaml config file
    config_path = './config/xetra-config.yml'
    with open(config_path, mode='rt', encoding='utf-8') as config_file:
        config = safe_load(config_file.read())

    # Configure logging
    log_config = config['logging']
    dictConfig(log_config)

    # Read s3 config
    s3_config = config['s3']

    # Read source and target config
    source_config = XetraSourceConfig(**config['source'])
    target_config = XetraTargetConfig(**config['target'])

    # Read meta config
    meta_config = config['meta']

    # Create S3 buckets
    src_name = 'deutsche-boerse-xetra-pds'
    trg_name = 'xetra-data-jt'

    src_bucket = S3BucketConnector(bucket_name=src_name,
        access_key=source_config['access_key'],
        secret_key=source_config['secret_key'],
        endpoint_url=source_config['endpoint_url']
    )

    trg_bucket = S3BucketConnector(bucket_name=trg_name,
        access_key=target_config['access_key'],
        secret_key=target_config['secret_key'],
        endpoint_url=target_config['endpoint_url']
    )

    # Create Xetra ETL job
    logger.info("Preparing to run the Xetra ETL job ...")
    xetra_etl = XetraETL(
        src_bucket=src_bucket,
        trg_bucket=trg_bucket,
        meta_key=meta_config['meta_key'],
        src_args=source_config,
        trg_args=target_config
    )

    xetra_etl.report()
    logger.info("Finished the Xetra ETL job!")


if __name__ == '__main__':
    main()
