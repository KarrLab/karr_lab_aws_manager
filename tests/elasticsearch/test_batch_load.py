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
        cls.src = batch_load.MongoToES(cache_dir=cls.cache_dir, index='test', verbose=True,
                                       profile_name='karrlab-zl')
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