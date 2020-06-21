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
        _source = {"includes": ["this", "that"], "excludes": ["them"]}
        result_0 = self.src._set_options(query_0, option_key_0, option_value_0)
        self.assertEqual(comp_0, result_0)
        result_1 = self.src._set_options(query_0, option_key_0, option_value_0, _source=_source)
        comp_1 = {'query': {'simple_query_string': {'query': 'something','fields': "field_0" }}, "_source":{"includes": ["this", "that"], "excludes": ["them"]}}
        self.assertEqual(result_1, comp_1)
    
    def test_build_simple_query_string_body(self):
        fields = ['field_0', 'field_1']
        analyze_wildcard = True
        result = self.src.build_simple_query_string_body('some query message',
                                            fields=fields, analyze_wildcard=analyze_wildcard,
                                            _source={"includes": ["something"]})
        comp ={'query': {'simple_query_string': {'query': 'some query message', 
        'fields': ['field_0', 'field_1'], 
        'flags': 'ALL', 'fuzzy_transpositions': True, 
        'fuzzy_max_expansions': 50, 'fuzzy_prefix_length': 0, 'minimum_should_match': 1, 
        'analyze_wildcard': True, 'lenient': True, 'quote_field_suffix': '', 
        'auto_generate_synonyms_phrase_query': True, 'default_operator': 'OR', 'analyzer': 'standard'}},
        "_source": {"includes": ["something"]}}
        self.assertEqual(result, comp)

    def test_build_bool_query_body(self):
        must = {'a': 0}
        _filter = [{'b': 1}, {'c': 2}]
        should = {'d': 3}
        must_not = {'e': 4}
        exp = {'query': {'bool': {'must': must, 'should': should, 'filter': _filter, 'must_not': must_not, 'minimum_should_match': 0}}}
        result = self.src.build_bool_query_body(must=must, _filter=_filter, should=should, must_not=must_not)
        self.assertEqual(result, exp)