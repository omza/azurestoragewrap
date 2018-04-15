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
from azurestoragewrap.exception import NameConventionError, AzureStorageWrapException

""" imports & Globals """
import time, os
import pytest

class BlobOne(StorageBlobModel):
    _encrypt = True
    _containername = 'blobtest'

    user = ''
    password = ''
    server = ''
    protocol = ''


""" Testcases positiv"""
class TestStorageBlobPositive(object):

    metadata = {
            'user':'bla', 
            'password':'blabla', 
            'server':'blablabla', 
            'protocol':'sbla'
        }

    def test_register_model(self):
        blob = StorageBlobContext(**testconfig)
        blob.register_model(BlobOne())
        assert 'BlobOne' in [model['modelname'] for model in blob._modeldefinitions]

    def test_upload_text(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blob = BlobOne(**self.__class__.metadata)
        blob.fromtext('Test Blob')
        container.upload(blob)

        assert blob.user == 'bla' and blob.content == b'Test Blob' and blob.exists()

    def test_upload_file(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        path_to_file = os.path.join(os.path.dirname(__file__), 'oliver.jpg')

        blob = BlobOne(**self.__class__.metadata)
        blob.fromfile(path_to_file)

        container.upload(blob)

        assert blob.user == 'bla' and blob.exists()

    def test_list_blobs(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            log.info(blob.name)

        assert len(blobs) == 2

    def test_download_blobs(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            log.info(blob.name)
            bloboneinstance = container.download(BlobOne(name=blob.name))
            assert bloboneinstance.content != None  and bloboneinstance.exists()
            
    def test_download_blobs_tofile(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            log.info(blob.name)
            bloboneinstance = container.download(BlobOne(name=blob.name))
            bloboneinstance.tofile(os.path.dirname(__file__),True)
            assert os.path.isfile(os.path.join(os.path.dirname(__file__), bloboneinstance.filename))
            
    def test_download_blobs_totext(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            log.info(blob.name)
            bloboneinstance = container.download(BlobOne(name=blob.name))
            if not bloboneinstance.properties.content_settings.content_encoding is None:
                blobtext = bloboneinstance.totext()
                assert blobtext == bloboneinstance.content.decode(bloboneinstance.properties.content_settings.content_encoding, 'ignore')
                break

    def test_blob_exists(self):

        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())
        blobs = container.list(BlobOne())
        
        for blob in blobs:
            exists = container.exists(BlobOne(name=blob.name))            
            assert exists
            break

    def test_delete_blob(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            log.info(blob.name)
            deleted = container.delete(BlobOne(name=blob.name))            
            assert deleted
            break
                
                                                                                                    
""" Testcases negative"""
class TestStorageBlobNegative(object):
    """ test if exceptions raised well """

    def test_error_file_replace(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            bloboneinstance = container.download(BlobOne(name=blob.name))
            with pytest.raises(AzureStorageWrapException):
                bloboneinstance.tofile(os.path.dirname(__file__))

    def test_error_text_encoding(self):
        container = StorageBlobContext(**testconfig)
        container.register_model(BlobOne())

        blobs = container.list(BlobOne())
        for blob in blobs:
            bloboneinstance = container.download(BlobOne(name=blob.name))
            bloboneinstance.properties.content_settings.content_encoding = None
            with pytest.raises(AzureStorageWrapException):
                blobtext = bloboneinstance.totext()


""" Testcases Housekeeping"""
class TestStorageBlobHousekeeping(object):

    def test_unregister_model_delete(self):
        blob = StorageBlobContext(**testconfig)

        blob.register_model(BlobOne())
        assert ('BlobOne' in [model['modelname'] for model in blob._modeldefinitions])

        blob.unregister_model(BlobOne(), delete_blob=True)
        assert (not 'BlobOne' in [model['modelname'] for model in blob._modeldefinitions])
