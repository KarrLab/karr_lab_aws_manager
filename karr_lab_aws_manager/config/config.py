from dotenv import load_dotenv
from pathlib import Path, PurePath
import boto3
import os


class credentialsFile:
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials', config_path='.wc/third_party/aws_config'):
        self.credential_path = credential_path
        self.config_path = config_path
        home_path = PurePath(Path.home(), self.credential_path)
        if os.path.exists(home_path):
            self.AWS_SHARED_CREDENTIALS_FILE = '~/' + self.credential_path
            self.AWS_CONFIG_FILE = '~/' + self.config_path
        else:
            self.AWS_SHARED_CREDENTIALS_FILE = '/' + self.credential_path
            self.AWS_CONFIG_FILE = '/' + self.config_path


class credentialsUser(credentialsFile):
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials', 
                config_path='.wc/third_party/aws_config', profile_name='test'):
        super().__init__(credential_path=credential_path, config_path='.wc/third_party/aws_config')
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = self.AWS_SHARED_CREDENTIALS_FILE
        os.environ['AWS_CONFIG_FILE'] = self.AWS_CONFIG_FILE
        self.session = boto3.Session(profile_name=profile_name)
        self.access_key = self.session.get_credentials().access_key
        self.secret_key = self.session.get_credentials().secret_key
