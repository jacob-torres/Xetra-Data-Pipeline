"""Classes and methods for accessing S3."""

from io import StringIO, BytesIO
from logging import getLogger
from os import environ

from boto3.session import Session
from pandas import DataFrame, read_csv

from xetra.common.constants import S3FileTypes


class S3BucketConnector():
    """Class for interacting with S3 buckets."""

    def __init__(self, bucket_name: str,
            access_key: str = environ['AWS_ACCESS_KEY_ID'],
            secret_key: str = environ['AWS_SECRET_ACCESS_KEY'],
            endpoint_url: str = 'https://s3.amazonaws.com'):
        """Instantiates the S3BucketConnector object.

        This object uses AWS credentials, an endpoint URL,
        and bucket name to access an S3 bucket.
        Note: AWS credentials default to environment variables.

        parameters
        ----------
        bucket_name : str
        The S3 bucket name

        access_key : str
        AWS access key credential (defaults to AWS_ACCESS_KEY_ID)

        secret_key : str
        AWS secret key credential (defaults to AWS_SECRET_ACCESS_KEY)

        endpoint_url : str
        Endpoint url for the S3 bucket (defaults to AWS S3 url)
        """

        self._name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url

        self.session = Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

        self._s3 = self.session.resource(
            service_name='s3',
            endpoint_url=self.endpoint_url
        )

        self._bucket = self._s3.Bucket(self._name)
        self._logger = getLogger(__name__)

    def list_files_by_prefix(self, prefix: str):
        """Generates a list of csv files for the given prefix.

        This method uses the given prefix to filter objects by date
        and returns a list of csv objects from the S3 bucket.

        parameters
        ----------
        prefix : str
        The date prefix of the objects

        returns
        -------
        files : list
        A list of files with the given prefix
        """

        files = [obj.key
            for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str,
            encoding: str = 'utf-8', sep: str = ','):
        """Reads data from an S3 object to a Pandas dataframe.

        parameters
        ----------
        key : str
        The key of the desired S3 object

        encoding : str
        The encoding to decode the file (defaults to 'utf-8')

        sep : str
        The separating character for parsing the file (defaults to ',')

        returns
        -------
        data_frame : DataFrame
        A Pandas dataframe containing the desired data
        """

        self._logger.info("Reading %s/%s/%s ...",
            self.endpoint_url, self._name, key)

        # Get csv file object from the bucket
        csv_obj = (
            self._bucket.Object(key=key).get()
            .get('Body').read().decode(encoding)
        )

        # Read the csv data to a dataframe
        data = StringIO(csv_obj)
        data_frame = read_csv(data, delimiter=sep)

        self._logger.info("Finished reading object %s.", key)
        return data_frame

    def write_df_to_s3(self, key: str,
            data_frame: DataFrame, format: str = 'csv'):
        """Writes dataframe to a target S3 bucket.

        parameters
        ----------
        key : str
        The object key

        data_frame : DataFrame
        The Pandas dataframe to convert into an S3 object

        format : str
        The format of the new S3 object (defaults to 'csv')
        Possible values : {'csv', 'parquet'}

        returns
        -------
        bool : True if the write was successful, False if not
        """

        self._logger.info("Preparing to write %s/%s/%s ...",
            self.endpoint_url, self._name, key)

        if format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_obj__(out_buffer, key)

        elif format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_obj__(out_buffer, key)

        # If the format is neither csv nor parquet
        self._logger.error(
            "Error: %s is not a valid file type. No files will be written.",
            format
        )

    def __put_obj__(self, out_buffer: StringIO or BytesIO, key: str):
        """Helper method for uploading objects to the S3 bucket.

        parameters
        ----------
        out_buffer : StringIO or BytesIO
        The output object for writing the file

        key : str
        The S3 object key

        returns
        -------
        bool : True if the upload was successful, False if not
        """

        new_obj = self._bucket.put_object(
            Body=out_buffer.getvalue(), Key=key
        )

        if not new_obj:
            self._logger.error(
                "Error: Something went wrong while writing %s/%s/%s.",
                self.endpoint_url, self._name, key
            )
            return False

        self._logger.info("Finished writing %s/%s/%s.",
            self.endpoint_url, self._name, key)
        return True
