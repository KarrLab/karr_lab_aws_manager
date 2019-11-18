import unittest
import tempfile
import shutil
from karr_lab_aws_manager.elasticsearch_kl import query_builder
from karr_lab_aws_manager.elasticsearch_kl import util as es_util
import requests
import time


class TestQuery(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.src = query_builder.QueryBuilder(profile_name='es-poweruser', credential_path='~/.wc/third_party/aws_credentials',
                config_path='~/.wc/third_party/aws_config', elastic_path='~/.wc/third_party/elasticsearch.ini',
                cache_dir=cls.cache_dir, service_name='es', max_entries=float('inf'), verbose=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)

    def test_set_options(self):
        query_0 = {'query': {'simple_query_string': {'query': 'something'}} }
        option_key_0 = 'fields'
        option_value_0 =  "field_0"
        comp_0 = {'query': {'simple_query_string': {'query': 'something','fields': "field_0" }} }
        result_0 = self.src._set_options(query_0, option_key_0, option_value_0)
        self.assertEqual(comp_0, result_0)
    
    def test_build_simple_query_string_body(self):
        fields = ['field_0', 'field_1']
        analyze_wildcard = 'true'
        result = self.src.build_simple_query_string_body('some query message',
                                            fields=fields, analyze_wildcard=analyze_wildcard)
        comp ={'query': {'simple_query_string': {'query': 'some query message', 
        'fields': ['field_0', 'field_1'], 
        'flags': 'ALL', 'fuzzy_transpositions': 'true', 
        'fuzzy_max_expansions': 50, 'fuzzy_prefix_length': 0, 'minimum_should_match': 1, 
        'analyze_wildcard': 'true', 'lenient': 'true', 'quote_field_suffix': '', 
        'auto_generate_synonyms_phrase_query': 'true', 'default_operator': 'AND', 'analyzer': 'standard'}}}
        self.assertEqual(result, comp)