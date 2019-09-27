import unittest
from karr_lab_aws_manager.elasticsearch import batch_load
from datanator_query_python.config import config
import tempfile
import shutil
import requests

class TestMongoToES(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.src = batch_load.MongoToES(cache_dir=cls.cache_dir, index='test', verbose=True)
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
        conf = config.Config()
        username = conf.USERNAME
        password = conf.PASSWORD
        server = conf.SERVER
        authDB = conf.AUTHDB
        db = 'datanator'
        count, _ = self.src.data_from_mongo_protein(server, db, username, password, authSource=authDB)
        self.assertTrue(count >= 1000)

    def test_make_action_and_metadata(self):
        _index = 1
        result = self.src.make_action_and_metadata(_index)
        self.assertEqual(result, {'index': { "_index" : self.src.index, "_id" : _index }})
    
    def test_data_to_es_bulk(self):
        cursor = [{'number': 0, 'mock_key_bulk': 'mock_value_0', 'uniprot_id': 'P0'},
                  {'number': 1, 'mock_key_bulk': 'mock_value_1', 'uniprot_id': 'P1'},
                  {'number': 2, 'mock_key_bulk': 'mock_value_2', 'uniprot_id': 'P2'},
                  {'number': 3, 'mock_key_bulk': 'mock_value_4', 'uniprot_id': 'P3'}]
        result = self.src.data_to_es_bulk(len(cursor), cursor, bulk_size=1)
        self.assertTrue(result <= {201, 200})

    def test_data_to_es_single(self):
        cursor = [{'mock_key': 'mock_value_0', 'another_mock_key': 'another_value_0', 'uniprot_id': 'P0'},
                  {'mock_key': 'mock_value_1', 'another_mock_key': 'another_value_0', 'uniprot_id': 'P1'}]
        result = self.src.data_to_es_single(len(cursor), cursor)
        self.assertTrue(result <= {201, 200})
