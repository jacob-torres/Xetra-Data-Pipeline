"""Classes and methods for accessing S3."""
import os
import boto3

class S3BucketConnector():
    """Class for interacting with S3 buckets."""

    def __init__(self, endpoint_url: str, bucket_name: str):
        """Instantiates the S3BucketConnector object.

        This object uses AWS credentials, an endpoint URL,
        and bucket information to access an S3 bucket.
        Note: AWS credentials are retrieved from environment variables.

        parameters
        ----------
        endpoint_url : str
        Endpoint url for S3 bucket

        bucket_name : str
        S3 bucket name
        """

        self.session = boto3.session.Session(
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            region=os.environ['AWS_DEFAULT_REGION']
        )

        self._s3 = self.session.resource(
            service_name='s3',
            endpoint_url=endpoint_url
        )

        self._bucket = self._s3.Bucket(bucket_name)


    def list_files_by_prefix(self, prefix: str):
        pass


    def read_csv_to_df(self):
        pass


    def write_df_to_s3(self):
        pass
