import unittest
from karr_lab_aws_manager.elasticsearch_kl import filters_util


class TestAnalyzersUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = filters_util.FiltersUtil()

    def test_init(self):
        self.assertNotEqual(str(self.src.cwd), '/') 

    def test_read_analyzer(self):
        rel_dir = 'karr_lab_aws_manager/elasticsearch_kl/filters/edge_ngram.json'
        data = self.src.read_filter(rel_dir)
        self.assertEqual(data['filter']['autocomplete_filter']['type'], 'edge_ngram')