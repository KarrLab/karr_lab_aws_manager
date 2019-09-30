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
        cls.destination = cls.cache_dir_source.split('/')[2]
        cls.src = util.QuiltUtil(credential_path='.wc/third_party/aws_credentials',
                                 config_path='.wc/third_party/aws_config', profile_name='karrlab-zl')
        cls.file = 'test_util.txt'
        cls.test_file = cls.cache_dir_source + cls.file
        Path(cls.test_file).touch()


    @classmethod 
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir_source)

    def test_add_to_package(self):
        source_0 = [self.test_file, self.cache_dir_source + '/', self.cache_dir_source + '/']
        destination_0 = [self.file,
                         self.destination + '/', self.file]
        meta_0 = [{'k_entry_0': 'v_entry_0'}, {
            'k_entry_1': 'v_entry_1'}, {'k_entry_2': 'v_entry_2'}]
        r_0 = self.src.add_to_package(destination_0, source_0, meta_0)
        self.assertEqual(
            r_0, '{} and {} must have the same suffix. Operation stopped at {}th element.'.format(destination_0[2], source_0[2], 2))
