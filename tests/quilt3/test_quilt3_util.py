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
        cls.src = util.QuiltUtil(aws_path='~/.wc/third_party/aws_credentials', config_path='~/.wc/third_party/aws_config',
                                 base_path=cls.credentials_cache, profile_name='quilt-karrlab',
                                 default_remote_registry='s3://karrlab', cache_dir=cls.credentials_cache)
        cls.file = 'test_util.txt'
        cls.test_file = cls.cache_dir_source + cls.file
        Path(cls.test_file).touch()
        cls.package_dest_0 = 'test.json'
        cls.package_dest_1 = 'test/'

    @classmethod 
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir_source)
        shutil.rmtree(cls.credentials_cache)
        cls.src.package.delete(cls.file)
        cls.src.package.delete(cls.package_dest_0)
        cls.src.package.delete(cls.package_dest_1[:-1])

    def test_bucket_obj(self):
        b = self.src.bucket_obj('s3://karrlab')
        self.assertTrue('karrlab/h1_hesc_data/LICENSE' in b.keys())

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

    @unittest.skip('reduce upload frequency')
    def test_push_to_remote(self):
        package = self.src.package
        package_name = 'karrlab/__test_quilt_util'
        remote_registry_0 = 's3://somenonsense'
        remote_registry_1 = 's3://karrlab'
        message = 'test package upload'
        r_0 = self.src.push_to_remote(package, package_name, destination=remote_registry_0, message=message)
        s_0 = "Invalid package destination path 's3://somenonsense'. 'dest', if set, must be a path in the 's3://karrlab' package registry specified by 'registry'."
        self.assertEqual(r_0, s_0)
        self.src.push_to_remote(package, package_name, registry=remote_registry_1, 
            destination=None, message=message)

        quilt3.delete_package(package_name, registry=remote_registry_1)

    def test_push_to_remote_custom_s3(self):
        src = util.QuiltUtil(aws_path='~/.wc/third_party/aws_credentials', config_path='~/.wc/third_party/aws_config',
                            base_path=self.credentials_cache, profile_name='s3-admin',
                            default_remote_registry='s3://karr-lab-aws-manager-test', cache_dir=self.credentials_cache)
        remote_registry_1 = 's3://karr-lab-aws-manager-test'
        package = self.src.package
        package_name = 'karrlab/__test_quilt_util'
        message = 'test package upload to custom s3'
        r = src.push_to_remote(package, package_name, destination=remote_registry_1, message=message)

    def test_build_from_external_bucket(self):
        key_0 = 'LICENSE'
        key_1 = 'quilt/docs/'
        bucket_name_0 = 'karr-lab-aws-manager-test'
        file_name_0_path = Path(self.credentials_cache)
        file_name_1_path = Path(self.credentials_cache)
        file_name_0 = str(file_name_0_path)
        file_name_1 = str(file_name_1_path)
        p_0 = self.src.build_from_external_bucket(self.package_dest_0, bucket_name_0, key_0, file_name_0,
                                                  profile_name='karrlab-zl')
        self.assertTrue(file_name_0_path.exists())
        self.assertTrue(p_0.__contains__(self.package_dest_0))
        p_1 = self.src.build_from_external_bucket(self.package_dest_1, bucket_name_0, key_1, file_name_1,
                                                  profile_name='karrlab-zl')
        self.assertTrue(file_name_1_path.exists())
        self.assertTrue(p_1.__contains__(self.package_dest_1 + 'CONTRIBUTING.md'))
