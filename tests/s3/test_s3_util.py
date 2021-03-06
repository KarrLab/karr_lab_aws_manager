import unittest
from karr_lab_aws_manager.s3 import util
from pathlib import Path
import tempfile
import shutil


class TestQuiltUtil(unittest.TestCase):

    @classmethod 
    def setUpClass(cls):
        cls.cache_dir_source = tempfile.mkdtemp()
        cls.src = util.S3Util(profile_name='s3-admin', credential_path='~/.wc/third_party/aws_credentials',
                              config_path='~/.wc/third_party/aws_config')

    @classmethod 
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir_source)

    @unittest.skip('reduce data transfer')
    def test_download_dir(self):
        dist_0 = 'quilt/docs'
        bucket = 'karr-lab-aws-manager-test'
        self.src.download_dir(dist_0, bucket, local=self.cache_dir_source)
        self.assertTrue(Path(self.cache_dir_source, dist_0, 'Installation.md').exists())

    #@unittest.skip('need to write copy function to repeat testing')
    def test_del_dir(self):
        bucket = 'karr-lab-aws-manager-test'
        directory = 'test/imgs'
        self.src.del_dir(bucket, directory)