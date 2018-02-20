""" config for Storage Emulator """
testconfig = {
    'AZURE_STORAGE_NAME': '',
    'AZURE_STORAGE_KEY': '',
    'AZURE_REQUIRE_ENCRYPTION': True,
    'AZURE_STORAGE_IS_EMULATED': True,
    'AZURE_KEY_IDENTIFIER': 'azurestoragewrap_test',
    'AZURE_SECRET_KEY': 'supa-dupa-secret-special-key2901'
}

""" logging while testing """
import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('azurestoragewrap')


""" Import application azurestoragewrap.table """        
from azurestoragewrap.queue import StorageQueueContext, StorageQueueModel


class QueueOne(StorageQueueModel):

    epgid = 0
    resolution = ''
    sourcefile = ''
    sourcelink = ''


""" Testcases positiv"""
class TestStorageQueuePositive(object):

    def test_register_model(self):
        queue = StorageQueueContext(**testconfig)
        queue.register_model(QueueOne())
        assert 'QueueOne' in queue._models


    def test_put(self):
        queue = StorageQueueContext(**testconfig)
        queue.register_model(QueueOne())        
        message = QueueOne(epgid = 1, resolution = 'HighDefinition')
        queue.put(message)

    def test_peek(self):
        queue = StorageQueueContext(**testconfig)
        queue.register_model(QueueOne())        
        message = QueueOne(epgid = 1, resolution = 'HighDefinition')
        message = queue.put(message)
        assert not message is None
        firstmessage = queue.peek(QueueOne())
        assert firstmessage.resolution == 'HighDefinition' 


