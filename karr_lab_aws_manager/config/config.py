from dotenv import load_dotenv
from pathlib import Path, PurePath
import boto3
import os


class credentialsFile:
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials'):
        self.credential_path = credential_path
        home_path = PurePath(Path.home(), self.credential_path)
        if os.path.exists(home_path):
            self.AWS_SHARED_CREDENTIALS_FILE = '~/' + self.credential_path
        else:
            self.AWS_SHARED_CREDENTIALS_FILE = "/.wc/third_party/aws_credentials"


class credentialsUser(credentialsFile):
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials', profile_name='test'):
        super().__init__(credential_path=credential_path)
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = self.AWS_SHARED_CREDENTIALS_FILE
        session = boto3.Session(profile_name=profile_name)
        self.access_key = session.get_credentials().access_key
        self.secret_key = session.get_credentials().secret_key
