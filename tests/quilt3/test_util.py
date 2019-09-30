import unittest
from karr_lab_aws_manager.quilt3 import util
import tempfile
import shutil
from pathlib import Path



class TestQuiltUtil(util.QuiltUtil):

    @classmethod 
    def setUpClass(cls):
        cls.cache_dir_source = tempfile.mkdtemp()
        cls.cache_dir_destination = tempfile.mkdtemp()
        cls.src = util.QuiltUtil(credential_path='.wc/third_party/aws_credentials',
                                 config_path='.wc/third_party/aws_config', profile_name='karrlab-zl')
        cls.file = 'test_util.txt'
        cls.test_file = cls.cache_dir_source + cls.file
        Path(cls.test_file).touch()


    @classmethod 
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir_source)
        shutil.rmtree(cls.cache_dir_destination)

    def test_add_to_package(self):
        source_0 = [cls.test_file, cls.cache_dir_source, cls.cache_dir_source]
        destination_0 = [self.cache_dir_destination+cls.file,
                         cls.cache_dir_destination, self.cache_dir_destination+cls.file]
        meta_0 = [{'k_entry_0': 'v_entry_0'}, {
            'k_entry_1': 'v_entry_1'}, {'k_entry_2': 'v_entry_2'}]
        r_0 = self.src.add_to_package(source_0, destination_0, meta_0)
        self.assertEqual(
            r_0, '{} and {} must have the same suffix. Operation stopped at {}th element.'.format(source_0[2], destination_0[2], meta_0[2]))
