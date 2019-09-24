import unittest
from karr_lab_aws_manager.config import config
import os

class TestConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.credentialsFile = config.credentialsFile()
        cls.credentialsFileX = config.credentialsFile(credential_path = 'some_nonsense', config_path='some_nonsense')
        cls.establishSession = config.establishSession()

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