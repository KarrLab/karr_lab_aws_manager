from botocore.exceptions import ClientError
import boto3
import logging
from requests_aws4auth import AWS4Auth
from karr_lab_aws_manager.config import config


class S3Util(config.establishS3):

    def __init__(self, profile_name=None, credential_path=None, config_path=None):
        ''' Interacting with aws s3 buckets
        '''
        super().__init__(profile_name=profile_name, credential_path=credential_path, config_path=config_path)


    def upload_file(file_name, bucket, object_name=None):
        """ Upload a file to an S3 bucket
            Args:
                file_name (:obj: `str`): File to upload
                bucket (:obj: `str`): Bucket to upload to
                object_name (:obj: `str`): S3 object name. If not specified then file_name is used
            Return:
                True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        try:
            response = self.client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
