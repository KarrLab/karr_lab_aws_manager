import unittest
from karr_lab_aws_manager.config import config

class TestConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.credentialsFile = config.credentialsFile()
        cls.credentialsUser = config.credentialsUser()

    def test_file_config(self):
        self.assertEqual(self.credentialsFile.AWS_SHARED_CREDENTIALS_FILE, '~/.wc/third_party/aws_credentials.ini')

    def test_user_config(self):
        self.assertEqual(self.credentialsUser.access_key, 'TESTKEYID')