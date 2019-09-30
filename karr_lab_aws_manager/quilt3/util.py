import quilt3
from karr_lab_aws_manager.config import config


class QuiltUtil(config.establishQuilt):

    def __init__(self, base_path=None, profile_name=None,
                default_remote_registry=None, aws_path=None):
        ''' Interacting with karrlab quilt3 bucket and packages
        '''
        super().__init__(base_path=base_path, profile_name=profile_name, 
                        default_remote_registry=default_remote_registry, aws_path=aws_path)

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