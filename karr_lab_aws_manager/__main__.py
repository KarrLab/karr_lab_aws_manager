""" karr_lab_aws_manager command line interface

:Author: Name <email>
:Date: 2019-9-16
:Copyright: 2019, Karr Lab
:License: MIT
"""

import cement
import tempfile
import karr_lab_aws_manager
import karr_lab_aws_manager.core
from karr_lab_aws_manager.elasticsearch_kl import util as es_util
import json
import pprint


class BaseController(cement.Controller):
    """ Base controller for command line application """

    class Meta:
        label = 'base'
        description = "karr_lab_aws_manager"
        arguments = [
            (['-v', '--version'], dict(action='version', version=karr_lab_aws_manager.__version__)),
        ]

    @cement.ex(help='command_1 description')
    def cmd1(self):
        """ command_1 description """
        print('command_1 output')

    @cement.ex(help='command_2 description')
    def cmd2(self):
        """ command_2 description """
        print('command_2 output')

    @cement.ex(hide=True)
    def _default(self):
        self._parser.print_help()


class Command3WithArgumentsController(cement.Controller):
    """ Command3 description """

    class Meta:
        label = 'command-3'
        description = 'Command3 description'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['arg_1'], dict(
                type=str, help='Description of arg_1')),
            (['arg_2'], dict(
                type=str, help='Description of arg_2')),
            (['--opt-arg-3'], dict(
                type=str, default='default value of opt-arg-1', help='Description of opt-arg-3')),
            (['--opt-arg-4'], dict(
                type=float, default=float('nan'), help='Description of opt-arg-4')),
        ]

    @cement.ex(hide=True)
    def _default(self):
        args = self.app.pargs
        args.arg_1
        args.arg_2
        args.opt_arg_3
        args.opt_arg_4


class EsDeleteIdx(cement.Controller):
    """Karrlab elasticsearch delete index. """

    class Meta:
        label = 'es-del-idx'
        description = 'Delete index in ES'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['index'], dict(
                type=str, help='Name of index in es')),
            (['--id'], dict(
                type=str, help='ID of the doc in index')),
            (['--profile_name', '-pn'], dict(
                type=str, default='es-poweruser',
                help='AWS profile to use for authentication')),
            (['--credential_path', '-cr'], dict(
                type=str, default='~/.wc/third_party/aws_credentials',
                help='Directory for aws credentials file')),
            (['--config_path', '-cp'], dict(
                type=str, default='~/.wc/third_party/aws_config',
                help='Directory for aws config file')
            ),
            (['--elastic_path', '-ep'], dict(
                type=str, default='~/.wc/third_party/elasticsearch.ini',
                help='Directory for file containing aws elasticsearch service variables'))
        ]

    @cement.ex(hide=True)
    def _default(self):
        ''' Delete elasticsearch index

            Args:
                index (:obj:`str`): name of index in es
                _id (:obj:`int`): id of the doc in index (optional)
        '''
        args = self.app.pargs
        es_util.EsUtil(profile_name=args.profile_name, credential_path=args.credential_path,
                       config_path=args.config_path, elastic_path=args.elastic_path).delete_index(args.index, _id=args.id)


class EsCheckSvr(cement.Controller):
    """Karrlab elasticsearch delete index. """

    class Meta:
        label = 'es-check-server'
        description = 'Check ES server health status'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--profile_name', '-pn'], dict(
                type=str, default='es-poweruser',
                help='AWS profile to use for authentication')),
            (['--credential_path', '-cr'], dict(
                type=str, default='~/.wc/third_party/aws_credentials',
                help='Directory for aws credentials file')),
            (['--config_path', '-cp'], dict(
                type=str, default='~/.wc/third_party/aws_config',
                help='Directory for aws config file')
            ),
            (['--elastic_path', '-ep'], dict(
                type=str, default='~/.wc/third_party/elasticsearch.ini',
                help='Directory for file containing aws elasticsearch service variables'))
        ]

    @cement.ex(hide=True)
    def _default(self):
        ''' Delete elasticsearch index

            Args:
                index (:obj:`str`): name of index in es
                _id (:obj:`int`): id of the doc in index (optional)
        '''
        args = self.app.pargs
        r = es_util.EsUtil(profile_name=args.profile_name, credential_path=args.credential_path,
                       config_path=args.config_path, elastic_path=args.elastic_path).index_health_status()
        print(r.content.decode('utf-8'))


class EsGetIdxMapping(cement.Controller):
    """Karrlab elasticsearch delete index. """

    class Meta:
        label = 'es-get-mapping'
        description = 'Check ES server health status'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--index'], dict(
                type=str, default='.kibana_1', help='Comma separated names of indices in es')),
            (['--profile_name', '-pn'], dict(
                type=str, default='es-poweruser',
                help='AWS profile to use for authentication')),
            (['--credential_path', '-cr'], dict(
                type=str, default='~/.wc/third_party/aws_credentials',
                help='Directory for aws credentials file')),
            (['--config_path', '-cp'], dict(
                type=str, default='~/.wc/third_party/aws_config',
                help='Directory for aws config file')
            ),
            (['--elastic_path', '-ep'], dict(
                type=str, default='~/.wc/third_party/elasticsearch.ini',
                help='Directory for file containing aws elasticsearch service variables'))
        ]

    @cement.ex(hide=True)
    def _default(self):
        ''' Delete elasticsearch index

            Args:
                index (:obj:`str`): name of index in es
                _id (:obj:`int`): id of the doc in index (optional)
        '''
        args = self.app.pargs
        r = es_util.EsUtil(profile_name=args.profile_name, credential_path=args.credential_path,
                       config_path=args.config_path, elastic_path=args.elastic_path).get_index_mapping(index=args.index)
        content = json.loads(r.content.decode('utf-8'))        
        pprint.pprint(content)

class EsBulkUpload(cement.Controller):
    """ Karrlab elasticsearch bulk upload cli """

    class Meta:
        label = 'es-bulk-upload'
        description = 'Bulk loading data into karrlab hosted elasticsearch service'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['cursor'], dict(
                type=str, help='Pymongo.Cursor/directory to files to be loaded')),
            (['id'], dict(
                type=str, help='Key in mongo collection for identification')),
            (['--count'], dict(
                type=int, default=None, help='Cursor/file size')),
            (['--index'], dict(
                type=str, default='test', help='Name of index in es')),
            (['--bulk_size', '-bz'], dict(
                type=int, default=100, help='Name of unique key to be used as index for es')),
            (['--profile_name', '-pn'], dict(
                type=str, default='es-poweruser',
                help='AWS profile to use for authentication')),
            (['--credential_path', '-cr'], dict(
                type=str, default='~/.wc/third_party/aws_credentials',
                help='Directory for aws credentials file')),
            (['--config_path', '-cp'], dict(
                type=str, default='~/.wc/third_party/aws_config',
                help='Directory for aws config file')
            ),
            (['--elastic_path', '-ep'], dict(
                type=str, default='~/.wc/third_party/elasticsearch.ini',
                help='Directory for file containing aws elasticsearch service variables')),
            (['--headers'], dict(
                type=dict, default={ "Content-Type": "application/json" },
                help='Http header'))
        ]

    @cement.ex(hide=True)
    def _default(self):
        ''' Load data into elasticsearch service

            Args:
                count (:obj:`int`): cursor size
                cursor (:obj:`pymongo.Cursor` or :obj:`iter`): documents to be PUT/POST to es
                index (:obj:`str`): name of unique key to be used as index for es
                bulk_size (:obj:`int`): number of documents in one PUT
                headers (:obj:`dict`): http header
                _id (:obj:`str`): key in mogno collection for identification

            Returns:
                (:obj:`set`): set of status codes
        '''
        args = self.app.pargs
        es_util.EsUtil(profile_name=args.profile_name, credential_path=args.credential_path,
                       config_path=args.config_path, elastic_path=args.elastic_path).data_to_es_bulk(
                       args.cursor, index=args.index, count=args.count, bulk_size=args.bulk_size,
                       _id=args.id, headers=args.headers)


class EsSetIdx(cement.Controller):
    """Karrlab elasticsearch settings for index. """

    class Meta:
        label = 'es-set-idx'
        description = 'Index settings in ES'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['index'], dict(
                type=str, help='Name of index in es')),
            (['replica_count'], dict(
                type=int, help='Number of replicas')),
            (['--shard_count', '-sc'], dict(
                type=int, default=1,
                help='Number of primary shards contained in the es cluster')
            ),
            (['--profile_name', '-pn'], dict(
                type=str, default='es-poweruser',
                help='AWS profile to use for authentication')),
            (['--credential_path', '-cr'], dict(
                type=str, default='~/.wc/third_party/aws_credentials',
                help='Directory for aws credentials file')),
            (['--config_path', '-cp'], dict(
                type=str, default='~/.wc/third_party/aws_config',
                help='Directory for aws config file')
            ),
            (['--elastic_path', '-ep'], dict(
                type=str, default='~/.wc/third_party/elasticsearch.ini',
                help='Directory for file containing aws elasticsearch service variables')),
            (['--headers'], dict(
                type=dict, default={ "Content-Type": "application/json" },
                help='Http header'))
        ]

    @cement.ex(hide=True)
    def _default(self):
        """Setting index's shard and replica number in es cluster
        
        Args:
            index (str): name of index to be set
            number_of_replicas (int): number of replica shards to be used for the index
            number_of_shards (int): number of primary shards contained in the es cluster
            headers (dict): http request content header description

        Returns:
            (HTTPResponse): http response
        """
        args = self.app.pargs
        es_util.EsUtil(profile_name=args.profile_name, credential_path=args.credential_path,
                       config_path=args.config_path, elastic_path=args.elastic_path).index_settings(
                           args.index, args.replica_count, number_of_shards=args.shard_count, headers=args.headers
                       )


class EsSetAnalysis(cement.Controller):
    """Karrlab elasticsearch settings for index. """

    class Meta:
        label = 'es-set-analysis'
        description = 'Index settings in ES'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['index'], dict(
                type=str, help='Name of index in es')),
            (['filter_dir'], dict(
                type=str, help='Directory of filter file')),
            (['analyzer_dir'], dict(
                type=str, help='Directory of analyzer file')
            ),
            (['--profile_name', '-pn'], dict(
                type=str, default='es-poweruser',
                help='AWS profile to use for authentication')),
            (['--credential_path', '-cr'], dict(
                type=str, default='~/.wc/third_party/aws_credentials',
                help='Directory for aws credentials file')),
            (['--config_path', '-cp'], dict(
                type=str, default='~/.wc/third_party/aws_config',
                help='Directory for aws config file')
            ),
            (['--elastic_path', '-ep'], dict(
                type=str, default='~/.wc/third_party/elasticsearch.ini',
                help='Directory for file containing aws elasticsearch service variables')),
            (['--headers'], dict(
                type=dict, default={ "Content-Type": "application/json" },
                help='Http header'))
        ]

    @cement.ex(hide=True)
    def _default(self):
        """Setting index's shard and replica number in es cluster
        
        Args:
            index (str): name of index to be set
            number_of_replicas (int): number of replica shards to be used for the index
            number_of_shards (int): number of primary shards contained in the es cluster
            headers (dict): http request content header description

        Returns:
            (HTTPResponse): http response
        """
        args = self.app.pargs
        es_util.EsUtil(profile_name=args.profile_name, credential_path=args.credential_path,
                       config_path=args.config_path, elastic_path=args.elastic_path).update_index_analysis(
                           args.index, args.filter_dir, args.analyzer_dir)




class App(cement.App):
    """ Command line application """
    class Meta:
        label = 'karr_lab_aws_manager'
        base_controller = 'base'
        handlers = [
            BaseController,
            Command3WithArgumentsController,
            EsBulkUpload,
            EsCheckSvr,
            EsGetIdxMapping,
            EsDeleteIdx,
            EsSetIdx,
            EsSetAnalysis
        ]

def main():
    with App() as app:
        app.run()

if __name__=='__main__':
    main()