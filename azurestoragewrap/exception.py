

class AzureStorageWrapException(Exception):
    """Basic exception for errors raised by azurestoragewrap lib"""

    def __init__(self, storagemodel=None, msg=None):
        if msg is None:
            # Set some default useful error message
            msg = 'An error occured within azurestoragewrap library'

        super(AzureStorageWrapException, self).__init__(msg)
        self.storagemodel = storagemodel


class NameConventionError(AzureStorageWrapException):
    """When you drive too fast"""
    def __init__(self, storagemodel=None):
        msg = '{!s} does not fit to the Azure Storage Name Conventions: https://blogs.msdn.microsoft.com/jmstall/2014/06/12/azure-storage-naming-rules/'.format(storagemodel)
        super(NameConventionError, self).__init__(storagemodel, msg)