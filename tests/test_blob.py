""" config """
from os import environ
testconfig = {
    'AZURE_STORAGE_NAME': environ.get('AZURE_STORAGE_NAME',''),
    'AZURE_STORAGE_KEY': environ.get('AZURE_STORAGE_KEY',''),
    'AZURE_REQUIRE_ENCRYPTION': False,
    'AZURE_STORAGE_IS_EMULATED': False,
    'AZURE_KEY_IDENTIFIER': 'azurestoragewrap_test',
    'AZURE_SECRET_KEY': 'supa-dupa-secret-special-key2901'
}

if testconfig['AZURE_STORAGE_NAME'] == '' and testconfig['AZURE_STORAGE_KEY'] == '':
    testconfig['AZURE_STORAGE_IS_EMULATED'] = True

""" logging while testing """
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('azurestoragewrap')


""" Import application azurestoragewrap.table """        
from azurestoragewrap.blob import StorageBlobContext, StorageBlobModel
from azurestoragewrap.exception import NameConventionError

""" imports & Globals """
import time, os
import pytest

class BlobOne(StorageBlobModel):
    _encrypt = False
    _containername = 'blobtest'

    user = ''
    password = ''
    server = ''
    protocol = ''


""" Testcases positiv"""
class TestStorageBlobPositive(object):

    def test_register_model(self):
        blob = StorageBlobContext(**testconfig)
        blob.register_model(BlobOne())
        assert 'BlobOne' in [model['modelname'] for model in blob._modeldefinitions]

    def test_upload_text(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blob = BlobOne('Test Blob', user='bla', password = 'blabla', server='blablabla', protocol='sbla')
        container.upload(blob)

        assert blob.user == 'bla' and blob.getblobsource() == 'Test Blob'

    def test_upload_file(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        path_to_file = os.path.join(os.path.dirname(__file__), 'oliver.jpg')

        blob = BlobOne(path_to_file, user='bla', password = 'blabla', server='blablabla', protocol='sbla')
        container.upload(blob)

        assert blob.user == 'bla' and blob.getblobsource() == path_to_file

    def test_list_blobs(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            log.info(blob)

        assert len(blobs) == 2


""" Testcases Housekeeping"""
class TestStorageBlobHousekeeping(object):

    def test_unregister_model_delete(self):
        blob = StorageBlobContext(**testconfig)

        blob.register_model(BlobOne())
        assert ('BlobOne' in [model['modelname'] for model in blob._modeldefinitions])

        blob.unregister_model(BlobOne(), delete_blob=True)
        assert (not 'BlobOne' in [model['modelname'] for model in blob._modeldefinitions])
