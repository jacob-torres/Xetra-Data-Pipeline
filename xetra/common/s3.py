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
        """Generates a list of csv files for the given prefix.
        
        This method uses the given prefix to filter objects by date
        and returns a list of csv objects from the S3 bucket.

        parameters
        ----------
        bucket : s3 bucket object
        The source S3 bucket to get the data

        prefix : str
        The date prefix of the desired objects

        returns
        -------
        files : list
        A list of files with the given prefix
        """

        files = [obj.key for obj in bucket.objects.filter(Prefix=prefix)]

        return files


    def read_csv_to_df(self, bucket: s3.Bucket,
        key: str, decoding: str='utf-8', sep: str=','):
        """Reads data from an S3 object to a Pandas dataframe.
    
        This method reads objects (of either .csv or .parquet format,)
        and loads the data into a Pandas dataframe for transformation.

        parameters
        ----------
        bucket : s3 bucket object
        The source S3 bucket to read from

        key : str
        The key of the desired S3 object

        decoding : str, default = 'utf-8'
        The decoding format to which to convert the S3 objects

        sep : str, default = ','
        The separating character for parsing the S3 object

        returns
        -------
        df : Dataframe
        The data in a Pandas dataframe
        """

        csv_obj = bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)

        return df


    def write_df_to_s3(self, bucket: s3.Bucket,
        key: str, df: pd.DataFrame, format: str='csv'):
        """Writes dataframe to a target S3 bucket.
        
        This method writes the transformed data report to the new S3 bucket.

        parameters
        ----------
        bucket : s3 bucket object
        The target S3 bucket to write the report to

        key : str
        The object key

        df : Dataframe
        The Pandas dataframe to convert into an S3 object

        format : str, default = 'csv'
        The format of the new S3 object

        returns
        -------
        bool
        True if the write was successful, False if not
        """

        out_buffer = BytesIO()

        if format == 'csv':
            df.to_csv(out_buffer, index=False)

        elif format == 'parquet':
            df.to_parquet(out_buffer, index=False)

        else:
            print(f"""Error: {format} is not a valid format.
                It should be either 'csv' or 'parquet.'""")

            return False

        bucket.put_object(Body=out_buffer.getvalue(), Key=key)

        return True
