import unittest
from karr_lab_aws_manager.elasticsearch_kl import index_setting_file


class TestIndexUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src = index_setting_file.IndexUtil()

    def test_init(self):
        self.assertNotEqual(str(self.src.cwd), '/') 

    def test_read_analyzer(self):
        rel_dir = '~/karr_lab/karr_lab_aws_manager/karr_lab_aws_manager/elasticsearch_kl/analyzers/auto_complete.json'
        data = self.src.read_analyzer(rel_dir)
        self.assertEqual(data['analyzer']['autocomplete']['type'], 'custom')