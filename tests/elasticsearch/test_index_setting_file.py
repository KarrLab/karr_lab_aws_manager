import unittest
from karr_lab_aws_manager.elasticsearch_kl import index_setting_file


class TestIndexUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.analyzer_dir = '~/karr_lab/karr_lab_aws_manager/karr_lab_aws_manager/elasticsearch_kl/analyzers/auto_complete.json'
        cls.filter_dir = '~/karr_lab/karr_lab_aws_manager/karr_lab_aws_manager/elasticsearch_kl/filters/autocomplete_filter.json'
        cls.mappings_dir = '~/karr_lab/karr_lab_aws_manager/karr_lab_aws_manager/elasticsearch_kl/mappings/protein.json'
        cls.src = index_setting_file.IndexUtil(filter_dir=cls.filter_dir, analyzer_dir=cls.analyzer_dir,
        mapping_properties_dir=cls.mappings_dir)

    def test_read_file(self):
        data = self.src.read_file(self.analyzer_dir)
        self.assertEqual(data['analyzer']['autocomplete']['type'], 'custom')

    def test_combine_file(self):
        result = self.src.combine_files(_filter=True, analyzer=True, mappings=True)
        self.assertIsNotNone(result['settings'].get('analysis'))