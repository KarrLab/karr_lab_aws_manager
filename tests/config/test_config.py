import unittest
from pathlib import Path
from karr_lab_aws_manager.config import config
import os
import quilt3
import tempfile
import shutil


class TestConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.establishSession = config.establishSession(credential_path='~/.wc/third_party/aws_credentials', 
                config_path='~/.wc/third_party/aws_config', profile_name='test')
        cls.cache_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)

    def test_file_config(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'abc'
        os.environ['AWS_PROFILE'] = 'profile'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'def'
        os.environ['AWS_DEFAULT_REGION'] = 'region'
        credentialsFile = config.credentialsFile()
        del os.environ['AWS_ACCESS_KEY_ID']
        del os.environ['AWS_PROFILE']
        del os.environ['AWS_SECRET_ACCESS_KEY']
        del os.environ['AWS_DEFAULT_REGION']
        credentialsFile = config.credentialsFile(credential_path='~/.wc/third_party/aws_credentials', config_path='~/.wc/third_party/aws_config',
                                                            profile_name='test')
        self.assertEqual(credentialsFile.AWS_SHARED_CREDENTIALS_FILE, str(Path('~/.wc/third_party/aws_credentials').expanduser()))
        self.assertEqual(credentialsFile.AWS_CONFIG_FILE, str(Path('~/.wc/third_party/aws_config').expanduser()))

    def test_session_config(self):
        self.assertEqual(self.establishSession.access_key, 'TESTKEYID')

    def test_es_config(self):
        establishES = config.establishES(credential_path='~/.wc/third_party/aws_credentials', 
                config_path='~/.wc/third_party/aws_config', profile_name='karrlab-zl',
                elastic_path='~/.wc/third_party/elasticsearch.ini', service_name='es')
        self.assertEqual(establishES.client.list_domain_names()['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertTrue(os.path.exists(os.path.expanduser(establishES.es_config)))
        self.assertTrue('datanator-elasticsearch' in establishES.es_endpoint)

    def test_s3_config(self):
        establishS3 = config.establishS3(profile_name='karrlab-zl', credential_path='~/.wc/third_party/aws_credentials',
                                         config_path='~/.wc/third_party/aws_config')
        self.assertEqual(establishS3.client.list_buckets()['Owner']['DisplayName'], 'zhouyang.lian')