""" logging """
import logging
log = logging.getLogger('azurestoragewrap')

class AzureStorageWrapException(Exception):
    """Basic exception for errors raised by azurestoragewrap lib"""

    def __init__(self, storagemodel=None, msg=None):
        if msg is None:
            # Set some default useful error message
            msg = 'An error occured within azurestoragewrap library'
        log.error(msg)

        super(AzureStorageWrapException, self).__init__(msg)
        self.storagemodel = storagemodel


class NameConventionError(AzureStorageWrapException):
    """If the Storage Object does not fit to Name conventions"""
    def __init__(self, storagemodel=None):
        msg = '{!s} does not fit to the Azure Storage Name Conventions: https://blogs.msdn.microsoft.com/jmstall/2014/06/12/azure-storage-naming-rules/'.format(storagemodel)
        super(NameConventionError, self).__init__(storagemodel, msg)

class ModelNotRegisteredError(AzureStorageWrapException):
    """If the Model is not registered"""
    def __init__(self, storagemodel=None):
        msg = 'Please register Model {!s} before useing it'.format(storagemodel)
        super(ModelNotRegisteredError, self).__init__(storagemodel, msg)

class ModelRegisteredMoreThanOnceError(AzureStorageWrapException):
    """If the Model is registered multiple times"""
    def __init__(self, storagemodel=None):
        msg = 'Something strange happend. Model {!s} is registered multipe times'.format(storagemodel)
        super(ModelRegisteredMoreThanOnceError, self).__init__(storagemodel, msg)