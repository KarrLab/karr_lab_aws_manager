import pkg_resources

# read version
with open(pkg_resources.resource_filename('karr_lab_aws_manager_test_0', 'VERSION'), 'r') as file:
    __version__ = file.read().strip()