import unittest
from karr_lab_aws_manager.elasticsearch import batch_load
import tempfile
import shutil

class TestMongoToES(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.src = batch_load.MongoToES(cache_dir=cls.cache_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)

    def test_connection(self):
        result = self.src.client.list_domain_names()
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertTrue('datanator-elasticsearch' in self.src.es_endpoint)

    def test_data_from_mongo(self):
        pass

    def test_data_to_es_bulk(self):
        cursor = [{'_id': 0, 'mock_key': 'mock_value_0'},
                  {'_id': 1, 'mock_key': 'mock_value_1'}]
        spec = { "index" : { "_index": "test", "_type" : "_doc", "_id" : "2" } }

    def test_data_to_es_single(self):
        cursor = [{'mock_key': 'mock_value_0', 'another_mock_key': 'another_value_0'},
                  {'mock_key': 'mock_value_1', 'another_mock_key': 'another_value_0'}]
        result = self.src.data_to_es_single(cursor, index='test_protein')
