import quilt3
from karr_lab_aws_manager.config import config
import json
from configparser import ConfigParser
from pathlib import Path, PurePath



class QuiltUtil:

    def __init__(self, base_path=None, profile_name=None, default_remote_registry=None,
                aws_path=None):
        ''' Handle Quilt authentication file creation without having to use quilt3.login()
            Args:
                aws_path (:obj: `str`): directory in which aws credentials file resides
                base_path (:obj: `str`): directory to store quilt3 credentials
                profile_name (:obj: `str`): AWS credentials profile name for quilt
                default_remote_registry (:obj: `str`): default remote registry to store quilt package
        '''
        base_path_obj = Path(Path.home(), base_path)
        aws_path_obj = Path(Path.home(), aws_path)
        quilt3.session.AUTH_PATH = base_path_obj / 'auth.json'
        quilt3.session.CREDENTIALS_PATH = base_path_obj / 'credentials.json'
        quilt3.session.AUTH_PATH.touch()
        self.auth_path = quilt3.session.AUTH_PATH
        self.quilt_credentials_path = quilt3.session.CREDENTIALS_PATH

        if aws_path_obj.exists():            
            aws_credentials_path = aws_path_obj / 'aws_credentials'
        else:
            aws_credentials_path = Path('/', base_path, 'aws_credentials.json')

        config = ConfigParser()
        config.read(aws_credentials_path)
        dic = {'access_key': config[profile_name]['aws_access_key_id'],
               'secret_key': config[profile_name]['aws_secret_access_key'],
               'token': None,
               'expiry_time': config[profile_name]['expiry_time']}
        with open(str(self.quilt_credentials_path), 'w') as f:
            json.dump(dic, f)
        quilt3.config(default_remote_registry=default_remote_registry)
        self.package = quilt3.Package()

    def add_to_package(self, destination=None, source=None, meta=None):
        ''' Specifically used for uploading datanator package to
            quilt3
            Args:
                source (:obj: `list` of :obj: `str`): sources to be added to package,
                                                      directories must end with '/'
                destination (:obj: `list` of :obj: `str` ): package(s) to be manipulated,
                                                            directories must end with '/'
                meta (:obj: `list` of :obj: `dict`): package meta
            Return:

        '''
        length = len(destination)
        if not (all(len(lst)) == length for lst in [source, meta]):
            return 'All three entries must be lists of the same length.'
        suffix = '/'
        for i, d in enumerate(destination):
            s = source[i]
            m = meta[i]
            if s.endswith(suffix) != d.endswith(suffix): # when s and d do not share the same suffix
                return '{} and {} must have the same suffix. Operation stopped at {}th element.'.format(d, s, i)

            if s.endswith(suffix):
                self.package.set_dir(d, s, meta=m)
            else:
                self.package.set(d, s, meta=m)

    def push_to_remote(self, package, package_name, destination=None, message=None):
        ''' Push local package to remote registry
            Args:
                package (:obj: `quilt3.Package()`): quilt pacakge
                package_name (:obj: `str`): name of package in "username/packagename" format
                destination (:obj: `str`): file landing destination in remote registry
                message (:obj: `str`): commit message
        '''
        try:
            package.push(package_name, dest=destination, message=message)
        except quilt3.util.QuiltException as e:
            return str(e)