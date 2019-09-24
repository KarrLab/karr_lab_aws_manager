import unittest
from karr_lab_aws_manager.elasticsearch import batch_load

class TestMongoToES(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = batch_load.MongoToES()

    def test_connection(self):
        result = self.src.client.list_domain_names()
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertTrue('datanator-elasticsearch' in self.src.es_endpoint)

    def test_data_from_mongo(self):
        pass

    def test_data_to_es(self):
        pass