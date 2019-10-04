import unittest
import tempfile
import shutil
from karr_lab_aws_manager.elasticsearch import query_builder
from karr_lab_aws_manager.elasticsearch import util as es_util
import requests
import time


class TestQuery(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.src = query_builder.QueryBuilder(profile_name='es-poweruser', credential_path='~/.wc/third_party/aws_credentials',
                config_path='~/.wc/third_party/aws_config', elastic_path='~/.wc/third_party/elasticsearch.ini',
                cache_dir=cls.cache_dir, service_name='es', max_entries=float('inf'), verbose=True)
        cls.es_manager = es_util.EsUtil(profile_name='es-poweruser', credential_path='~/.wc/third_party/aws_credentials',
                config_path='~/.wc/third_party/aws_config', elastic_path='~/.wc/third_party/elasticsearch.ini',
                cache_dir=cls.cache_dir, service_name='es', max_entries=float('inf'), verbose=True)
        
        cls.index_0 = 'test_0'
        cls.index_1 = 'test_1'
        cls.url_0 = cls.src.es_endpoint + '/' + cls.index_0
        cls.url_1 = cls.src.es_endpoint + '/' + cls.index_1
        cursor_0 = [{'number': 0, 'mock_key_bulk': 'mock_value_0', 'uniprot_id': 'P0'},
                  {'number': 1, 'mock_key_bulk': 'mock_value_1', 'uniprot_id': 'P1'},
                  {'number': 2, 'mock_key_bulk': 'mock_value_2', 'uniprot_id': 'P2'},
                  {'number': 3, 'mock_key_bulk': 'mock_value_3', 'uniprot_id': 'P3'}]
        cursor_1 = [{'number': 4, 'mock_key_bulk': 'mock_value_4', 'uniprot_id': 'P4'},
                  {'number': 5, 'mock_key_bulk': 'mock_value_5', 'uniprot_id': 'P5'},
                  {'number': 6, 'mock_key_bulk': 'mock_value_6', 'uniprot_id': 'P6'},
                  {'number': 7, 'mock_key_bulk': 'mock_value_7', 'uniprot_id': 'P7'},
                  {'number': 8, 'mock_key_bulk': 'mock_value_7', 'uniprot_id': 'P8'}]
        _ = cls.es_manager.data_to_es_bulk(len(cursor_0), cursor_0, cls.index_0, bulk_size=1)
        _ = cls.es_manager.data_to_es_bulk(len(cursor_0), cursor_1, cls.index_1, bulk_size=1)
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)
        requests.delete(cls.url_0, auth=cls.src.awsauth)
        requests.delete(cls.url_1, auth=cls.src.awsauth)

    def test_set_options(self):
        query_0 = {'query': {'simple_query_string': {'query': 'something'}} }
        option_key_0 = 'fields'
        option_value_0 =  "field_0"
        comp_0 = {'query': {'simple_query_string': {'query': 'something','fields': "field_0" }} }
        result_0 = self.src._set_options(query_0, option_key_0, option_value_0)
        self.assertEqual(comp_0, result_0)
    
    def  test_build_simple_query_string_body(self):
        fields = ['field_0', 'field_1']
        analyze_wildcard = True
        result = self.src.build_simple_query_string_body('some query message',
                                            fields=fields, analyze_wildcard=analyze_wildcard)
        comp = {
            "query": {
                "simple_query_string": {
                    "query": 'some query message',
                    "fields": ['field_0', 'field_1'],
                    "flags": "ALL",
                    "fuzzy_transpositions": True,
                    "fuzzy_max_expansions": 50,
                    "fuzzy_prefix_length": 0,
                    "minimum_should_match": 1,
                    "default_operator": "or",
                    "analyzer": "standard",
                    "lenient": False,
                    "quote_field_suffix": "",
                    "analyze_wildcard": True,
                    "auto_generate_synonyms_phrase_query": True
                }
            }
        }
        self.assertEqual(result, comp)

    def test_request_body_search(self):
        query_0 = 'mock_value_3'
        field_0 = ['mock_key_bulk', 'number']
        body_0 = self.src.build_simple_query_string_body(query_0, fields=field_0, lenient=True,
        analyze_wild_card=True)
        index_0 = 'test_0,test_1'
        es = self.src._build_es()
        es_0 = es.search(index=index_0, body=body_0)
        self.assertEqual(es_0['hits']['hits'][0]['_source'], {'number': 3, 'mock_key_bulk': 'mock_value_3', 'uniprot_id': 'P3'})