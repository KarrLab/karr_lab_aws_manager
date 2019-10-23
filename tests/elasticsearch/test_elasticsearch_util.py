import unittest
from karr_lab_aws_manager.elasticsearch_kl import util
from datanator_query_python.config import config
from pathlib import Path
import json
import tempfile
import shutil
import requests
import json


class TestMongoToES(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.src = util.EsUtil(profile_name='es-poweruser', credential_path='~/.wc/third_party/aws_credentials',
                config_path='~/.wc/third_party/aws_config', elastic_path='~/.wc/third_party/elasticsearch.ini',
                cache_dir=cls.cache_dir, service_name='es', max_entries=float('inf'), verbose=True)
        cls.index = 'test'
        cls.url = cls.src.es_endpoint + '/' + cls.index
        requests.delete(cls.url, auth=cls.src.awsauth)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)
        requests.delete(cls.url, auth=cls.src.awsauth)

    def test_build_es(self):
        result_0 = self.src._build_es()
        self.assertTrue(hasattr(result_0, 'msearch_template'))
    
    def test_connection(self):
        result = self.src.client.list_domain_names()
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertTrue('datanator-elasticsearch' in self.src.es_endpoint)

    def test_make_action_and_metadata(self):
        _id = '1'
        result = self.src.make_action_and_metadata(self.index, _id)
        self.assertEqual(result, {'index': { "_index" : self.index, "_id" : _id }})

    def test_unassigned_reason(self):
        result = self.src.unassigned_reason()
        self.assertEqual(result.status_code, 200)
    
    def test_data_to_es_bulk(self):
        cursor = [{'number': 0, 'mock_key_bulk': 'mock_value_0', 'uniprot_id': 'P0'},
                  {'number': 1, 'mock_key_bulk': 'mock_value_1', 'uniprot_id': 'P1'},
                  {'number': 2, 'mock_key_bulk': 'mock_value_2', 'uniprot_id': 'P2'},
                  {'number': 3, 'mock_key_bulk': 'mock_value_4', 'uniprot_id': 'P3'}]
        result = self.src.data_to_es_bulk(len(cursor), cursor, self.index, bulk_size=1)
        self.assertTrue(result <= {201, 200})
        result = self.src.index_settings(self.index, 0)
        self.assertEqual(result.text, '{"acknowledged":true}')
        for dic in cursor:
            p = Path(self.cache_dir).joinpath(dic['uniprot_id'] + '.json')
            with p.open(mode='w+') as f:
                json.dump(dic, f)
        result = self.src.data_to_es_bulk(len(cursor), self.cache_dir, self.index, bulk_size=1)
        self.assertTrue(result <= {201, 200})

    # @unittest.skip('reduce debugging confusion')
    def test_data_to_es_single(self):
        cursor = [{'mock_key': 'mock_value_0', 'another_mock_key': 'another_value_0', 'uniprot_id': 'P0'},
                  {'mock_key': 'mock_value_1', 'another_mock_key': 'another_value_0', 'uniprot_id': 'P1'}]
        result = self.src.data_to_es_single(len(cursor), cursor, self.index)
        self.assertTrue(result <= {201, 200})

    # @unittest.skip('reduce debugging confusion')
    def test_delete_index(self):
        cursor = [{'number': 0, 'mock_key_bulk': 'mock_value_0', 'uniprot_id': 'P0'},
                  {'number': 1, 'mock_key_bulk': 'mock_value_1', 'uniprot_id': 'P1'},
                  {'number': 2, 'mock_key_bulk': 'mock_value_2', 'uniprot_id': 'P2'},
                  {'number': 3, 'mock_key_bulk': 'mock_value_4', 'uniprot_id': 'P3'}]
        _ = self.src.data_to_es_bulk(len(cursor), cursor, self.index, bulk_size=1)
        _id_0 = None
        _id_1 = 'P1'
        status_0 = self.src.delete_index(self.index, _id_1)
        self.assertTrue(status_0 == 200)
        status_1 = self.src.delete_index(self.index)
        self.assertEqual(status_1, 200)