from pathlib import Path, PurePath
import boto3
import quilt3
import json
import os
from configparser import ConfigParser

class credentialsFile:
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials', config_path='.wc/third_party/aws_config'):
        ''' Establish environment variables' paths
        '''
        self.credential_path = credential_path
        self.config_path = config_path
        self.home_path = PurePath(Path.home(), self.credential_path)
        if os.path.exists(self.home_path):
            self.AWS_SHARED_CREDENTIALS_FILE = '~/' + self.credential_path
            self.AWS_CONFIG_FILE = '~/' + self.config_path
        else:
            self.AWS_SHARED_CREDENTIALS_FILE = '/' + self.credential_path
            self.AWS_CONFIG_FILE = '/' + self.config_path


class establishSession(credentialsFile):
    
    def __init__(self, credential_path='.wc/third_party/aws_credentials', 
                config_path='.wc/third_party/aws_config', profile_name='test'):
        super().__init__(credential_path=credential_path, config_path=config_path)
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = self.AWS_SHARED_CREDENTIALS_FILE
        os.environ['AWS_CONFIG_FILE'] = self.AWS_CONFIG_FILE
        self.session = boto3.Session(profile_name=profile_name)
        self.access_key = self.session.get_credentials().access_key
        self.secret_key = self.session.get_credentials().secret_key


class establishES(establishSession):

    def __init__(self, credential_path='.wc/third_party/aws_credentials', 
                config_path='.wc/third_party/aws_config', profile_name='test',
                elastic_path='.wc/third_party/elasticsearch.ini', service_name='es'):
        super().__init__(credential_path=credential_path, config_path=config_path,
                        profile_name=profile_name)
        self.client = self.session.client(service_name)
        if os.path.exists(self.home_path):
            self.es_config = '~/' + elastic_path
        else:
            self.es_config = '/' + elastic_path
        config = ConfigParser()
        config.read(os.path.expanduser(self.es_config))
        self.es_endpoint = config['elasticsearch-endpoint']['address']
        self.region = self.session.region_name


class establishS3(establishSession):

    def __init__(self, credential_path=None, config_path=None, profile_name=None, service_name='s3'):
        super().__init__(credential_path=credential_path, config_path=config_path, profile_name=profile_name)
        self.client = self.session.client(service_name)
        self.region = self.session.region_name


class establishQuilt:

    def __init__(self, base_path=None, profile_name=None, default_remote_registry=None,
                aws_path=None):
        ''' Handle Quilt authentication file creation without having to use quilt3.login()
            Args:
                aws_path (:obj: `str`): directory in which aws credentials file resides
                base_path (:obj: `str`): directory to store quilt3 credentials
                profile_name (:obj: `str`): AWS credentials profile name for quilt
                default_remote_registry (:obj: `str`): default remote registry to store quilt package
        '''
        base_path_obj = Path(Path.home(), base_path)
        aws_path_obj = Path(Path.home(), aws_path)
        quilt3.session.AUTH_PATH = base_path_obj / 'auth.json'
        quilt3.session.CREDENTIALS_PATH = base_path_obj / 'credentials.json'
        quilt3.session.AUTH_PATH.touch()
        self.auth_path = quilt3.session.AUTH_PATH
        self.quilt_credentials_path = quilt3.session.CREDENTIALS_PATH

        if aws_path_obj.exists():            
            aws_credentials_path = aws_path_obj / 'aws_credentials'
        else:
            aws_credentials_path = Path('/', base_path, 'aws_credentials.json')

        config = ConfigParser()
        config.read(aws_credentials_path)
        dic = {'access_key': config[profile_name]['aws_access_key_id'],
               'secret_key': config[profile_name]['aws_secret_access_key'],
               'token': None,
               'expiry_time': config[profile_name]['expiry_time']}
        with open(str(self.quilt_credentials_path), 'w') as f:
            json.dump(dic, f)
        quilt3.config(default_remote_registry=default_remote_registry)
        self.package = quilt3.Package()