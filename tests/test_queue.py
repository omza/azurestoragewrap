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
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('azurestoragewrap')

log.debug('AZURE_STORAGE_NAME={!s}'.format(testconfig['AZURE_STORAGE_NAME']))
log.debug('AZURE_STORAGE_KEY={!s}'.format(testconfig['AZURE_STORAGE_KEY']))


""" Import application azurestoragewrap.table """        
from azurestoragewrap.queue import StorageQueueContext, StorageQueueModel


""" imports & Globals """
import time

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
        message = QueueOne(epgid = 1, resolution = 'test_put')
        queue.put(message)
        queue.delete(message)

    def test_peek(self):
        queue = StorageQueueContext(**testconfig)
        queue.register_model(QueueOne())        
        message = QueueOne(epgid = 1, resolution = 'test_peek')
        message = queue.put(message)
        assert not message is None

        firstmessage = queue.peek(QueueOne())
        assert firstmessage.resolution == 'test_peek'

        queue.delete(message)
        

    def test_get_hide(self):
        queue = StorageQueueContext(**testconfig)
        queue.register_model(QueueOne())        
        message = QueueOne(epgid = 1, resolution = 'test_get_hide')
        queue.put(message)

        getmessage = queue.get(QueueOne(), hide=10)
        assert getmessage.epgid == 1 and getmessage.resolution == 'test_get_hide'

        testmessage = queue.get(QueueOne())
        assert testmessage is None

        time.sleep(10)
        getmessage = queue.get(QueueOne())
        assert getmessage.epgid == 1 and getmessage.resolution == 'test_get_hide'
        queue.delete(getmessage)


    def test_get_nothide(self):
        queue = StorageQueueContext(**testconfig)
        queue.register_model(QueueOne())        
        message = QueueOne(epgid = 1, resolution = 'test_get_nothide')
        queue.put(message)

        getmessage = queue.get(QueueOne(), hide=1)
        assert getmessage.epgid == 1 and getmessage.resolution == 'test_get_nothide'

        time.sleep(1)

        testmessage = queue.get(QueueOne())
        assert testmessage.epgid == 1 and getmessage.resolution == 'test_get_nothide'

        queue.delete(testmessage)

    def test_unregister_model(self):
        queue = StorageQueueContext(**testconfig)
        message = QueueOne() 
        queue.register_model(message)
        assert message.__class__.__name__ in queue._models

        queue.unregister_model(message)
        assert not message.__class__.__name__ in queue._models

""" Testcases Housekeeping"""
class TestStorageQueueHousekeeping(object):

    def test_unregister_model_delete(self):
        queue = StorageQueueContext(**testconfig)
        message = QueueOne() 
        queue.register_model(message)
        assert message.__class__.__name__ in queue._models

        queue.unregister_model(message, delete_queue=True)
        assert not message.__class__.__name__ in queue._models        




