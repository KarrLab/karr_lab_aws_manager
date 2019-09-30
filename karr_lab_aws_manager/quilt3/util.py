import quilt3
from karr_lab_aws_manager.s3 import util as s3_util


class QuiltUtil(s3_util.S3Util):

    def __init__(self, credential_path=None, config_path=None, profile_name=None):
        ''' Interacting with karrlab quilt3 bucket and packages
        '''
        super().__init__(credential_path=credential_path, config_path=config_path, profile_name=profile_name)
        self.package = quilt3.Package()

    def add_to_package(self, source=None, destination=None, meta=None):
        ''' Specifically used for uploading datanator package to
            quilt3
            Args:
                source (:obj: `list` of :obj: `str`): sources to be added to package
                destination (:obj: `list` of :obj: `str` ): package(s) to be manipulated
                meta (:obj: `list` of :obj: `dict`): package meta
            Return:

        '''
        length = len(source)
        if not (all(len(lst)) == length for lst in [destination, meta]):
            return 'All three entries must be lists of the same length.'
        suffix = '/'
        for i, s in enumerate(source):
            d = destination[i]
            m = meta[i]
            if s.endswith(suffix) != d.endswith(suffix): # when s and d do not share the same suffix
                return '{} and {} must have the same suffix. Operation stopped at {}th element.'.format(s, d, i)

            if s.endswith(suffix):
                self.package.set_dir(s, d, meta=m)
            else:
                self.package.set(s, d, meta=m)
