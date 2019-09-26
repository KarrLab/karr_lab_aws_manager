import unittest
from karr_lab_aws_manager.elasticsearch import batch_load
import tempfile
import shutil
import requests

class TestMongoToES(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.src = batch_load.MongoToES(cache_dir=cls.cache_dir, index='test')
        cls.url = cls.src.es_endpoint + '/' + cls.src.index
        requests.delete(cls.url, auth=cls.src.awsauth)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)
        requests.delete(cls.url, auth=cls.src.awsauth)

    def test_connection(self):
        result = self.src.client.list_domain_names()
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertTrue('datanator-elasticsearch' in self.src.es_endpoint)

    def test_data_from_mongo(self):
        pass

    def test_make_action_and_metadata(self):
        _index = 1
        result = self.src.make_action_and_metadata(_index)
        self.assertEqual(result, {'index': { "_index" : self.src.index, "_id" : _index }})
    
    def test_data_to_es_bulk(self):
        cursor = [{'number': 0, 'mock_key_bulk': 'mock_value_0'},
                  {'number': 1, 'mock_key_bulk': 'mock_value_1'}]
        result = self.src.data_to_es_bulk(cursor)
        self.assertTrue(result <= {201, 200})

    def test_data_to_es_single(self):
        cursor = [{'mock_key': 'mock_value_0', 'another_mock_key': 'another_value_0'},
                  {'mock_key': 'mock_value_1', 'another_mock_key': 'another_value_0'}]
        result = self.src.data_to_es_single(cursor)
        self.assertTrue(result <= {201, 200})
