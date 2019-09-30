import unittest
from karr_lab_aws_manager.config import config
import os
import quilt3
import tempfile
import shutil


class TestConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.credentialsFile = config.credentialsFile()
        cls.credentialsFileX = config.credentialsFile(credential_path = 'some_nonsense', config_path='some_nonsense')
        cls.establishSession = config.establishSession()
        cls.cache_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)

    def test_file_config(self):
        self.assertEqual(self.credentialsFile.AWS_SHARED_CREDENTIALS_FILE, '~/.wc/third_party/aws_credentials')
        self.assertEqual(self.credentialsFile.AWS_CONFIG_FILE, '~/.wc/third_party/aws_config')
        self.assertEqual(self.credentialsFileX.AWS_SHARED_CREDENTIALS_FILE, "/some_nonsense")
        self.assertEqual(self.credentialsFileX.AWS_CONFIG_FILE, "/some_nonsense")

    def test_session_config(self):
        self.assertEqual(self.establishSession.access_key, 'TESTKEYID')

    def test_es_config(self):
        establishES = config.establishES(profile_name='karrlab-zl', service_name='es')
        self.assertEqual(establishES.client.list_domain_names()['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertTrue(os.path.exists(os.path.expanduser(establishES.es_config)))
        self.assertTrue('datanator-elasticsearch' in establishES.es_endpoint)

    def test_s3_config(self):
        establishS3 = config.establishS3(profile_name='karrlab-zl', credential_path='.wc/third_party/aws_credentials',
                                         config_path='.wc/third_party/aws_config')
        self.assertEqual(establishS3.client.list_buckets()['Owner']['DisplayName'], 'zhouyang.lian')

    def test_quilt_config(self):
        establishQuilt = config.establishQuilt(base_path=self.cache_dir, profile_name='quilt-s3',
                                               default_remote_registry='s3://quilt-karrlab', aws_path='.wc/third_party')
        self.assertTrue(establishQuilt.quilt_credentials_path.exists())

        