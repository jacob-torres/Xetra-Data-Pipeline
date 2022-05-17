"""Runs the Xetra ETL application."""
import logging
from logging.config import dictConfig
from yaml import safe_load

def main():
    """Entry-point for running the Xetra ETL application."""

    # Parse the yaml config file
    config_path = './config/xetra_report_config.yml'
    with open(config_path, mode='rt', encoding='utf-8') as config_file:
        config_obj = safe_load(config_file.read())
        log_config = config_obj['logging']
        dictConfig(log_config)


if __name__ == '__main__':
    main()
