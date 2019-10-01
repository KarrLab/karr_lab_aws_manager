import unittest
from karr_lab_aws_manager.quilt3 import util
import tempfile
import shutil
import quilt3
import os
from pathlib import Path



class TestQuiltUtil(unittest.TestCase):

    @classmethod 
    def setUpClass(cls):
        cls.cache_dir_source = tempfile.mkdtemp()
        cls.credentials_cache = tempfile.mkdtemp()
        cls.destination = cls.cache_dir_source.split('/')[2]
        cls.src = util.QuiltUtil(aws_path='.wc/third_party',
                                 base_path=cls.credentials_cache, profile_name='quilt-s3',
                                 default_remote_registry='s3://karrlab')
        cls.file = 'test_util.txt'
        cls.test_file = cls.cache_dir_source + cls.file
        Path(cls.test_file).touch()


    @classmethod 
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir_source)
        shutil.rmtree(cls.credentials_cache)
        cls.src.package.delete(cls.file)

    def test_init(self):
        self.assertTrue(self.src.quilt_credentials_path.exists())

    def test_add_to_package(self):
        source_0 = [self.test_file, self.cache_dir_source + '/', self.cache_dir_source + '/']
        destination_0 = [self.file,
                         self.destination + '/', self.file]
        meta_0 = [{'k_entry_0': 'v_entry_0'}, {
            'k_entry_1': 'v_entry_1'}, {'k_entry_2': 'v_entry_2'}]
        r_0 = self.src.add_to_package(destination_0, source_0, meta_0)
        self.assertEqual(
            r_0, '{} and {} must have the same suffix. Operation stopped at {}th element.'.format(destination_0[2], source_0[2], 2))

    def test_push_to_remote(self):
        package = self.src.package
        package_name = 'karrlab/test'
        remote_registry_0 = 's3://somenonsense'
        remote_registry_1 = 's3://karrlab/test'
        message = 'test package upload'
        r_0 = self.src.push_to_remote(package, package_name, destination=remote_registry_0, message=message)
        s_0 = "Invalid package destination path 's3://somenonsense'. 'dest', if set, must be a path in the 's3://karrlab' package registry specified by 'registry'."
        self.assertEqual(r_0, s_0)
        self.src.push_to_remote(package, package_name, destination=remote_registry_1, message=message)
