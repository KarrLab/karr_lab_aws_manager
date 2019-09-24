import datanator_query_python
from karr_lab_aws_manager.config import config


class MongoToES:

    def __init__(self, profile_name='karrlab-zl', service_name='es'):
        credential_manager = config.credentialsUser(profile_name=profile_name)
        self.client = credential_manager.session.client(service_name)
        