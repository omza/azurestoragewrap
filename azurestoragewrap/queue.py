""" imports & globals """
from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage.queue import QueueService, QueueMessage

import datetime
from ast import literal_eval


""" helpers """
from azurestoragewrap.snippets import safe_cast

""" encryption """
from azurestoragewrap.encryption import (
    KeyWrapper,
    KeyResolver    
    )

""" logging """
import logging
log = logging.getLogger('azurestoragewrap')


""" model base classes """
class StorageQueueModel(QueueMessage):
    _queuename = ''
    _dateformat = ''
    _datetimeformat = ''
    
    def __init__(self, **kwargs):                  
        """ constructor """
        super().__init__()
        self._queuename = self.__class__._queuename
        self._dateformat = self.__class__._dateformat
        self._datetimeformat = self.__class__._datetimeformat
               
        """ parse **kwargs in tmp dict var """
        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '':
                if (not key in vars(QueueMessage).items()):
                    if key in kwargs:                   
                        value = kwargs.get(key)
                        to_type = type(default)              
                        if to_type is StorageTableCollection:
                            setattr(self, key, value)
                        elif to_type is datetime.datetime:
                            setattr(self, key, safe_cast(value, to_type, default, self._datetimeformat))
                        elif to_type is datetime.date:
                            setattr(self, key, safe_cast(value, to_type, default, self._dateformat))
                        else:
                            setattr(self, key, safe_cast(value, to_type, default))               
                    else:
                        setattr(self, key, default)

    def getmessage(self) -> str:
        """ parse self into unicode string as message content """   
        image = {}
        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key !='':                                      
                image[key] = getattr(self, key, default)                                  
        return str(image)

    def mergemessage(self, message):
        """ parse OueueMessage in Model vars """
        if isinstance(message, QueueMessage):
            """ merge queue message vars """
            for key, value in vars(message).items():
                setattr(self, key, value)

            """ parse message content """
            try:
                content = literal_eval(message.content)
                for key, value in content.items():
                    setattr(self, key, value)

            except:
                log.exception('cant parse message {} into attributes.'.format(message.content))
        pass

""" wrapper classes """
class StorageQueueContext():
    """Initializes the repository with the specified settings dict.
        Required settings in config dict are:
        - AZURE_STORAGE_NAME
        - AZURE_STORAGE_KEY
    """
    
    _models = []
    _service = None
    _storage_key = ''
    _storage_name = ''

    def __init__(self, **kwargs):

        self._storage_name = kwargs.get('AZURE_STORAGE_NAME', '')
        self._storage_key = kwargs.get('AZURE_STORAGE_KEY', '')

        """ service init """
        if self._storage_key != '' and self._storage_name != '':
            self._service = QueueService(account_name = self._storage_name, account_key = self._storage_key, protocol='https')

        """ registered models """
        self._models = []

        """ encrypt queue service """
        if kwargs.get('AZURE_REQUIRE_ENCRYPTION', False):

            # Create the KEK used for encryption.
            # KeyWrapper is the provided sample implementation, but the user may use their own object as long as it implements the interface above.
            kek = KeyWrapper(kwargs.get('AZURE_KEY_IDENTIFIER', 'otrrentapi'), kwargs.get('SECRET_KEY', 'super-duper-secret')) # Key identifier

            # Create the key resolver used for decryption.
            # KeyResolver is the provided sample implementation, but the user may use whatever implementation they choose so long as the function set on the service object behaves appropriately.
            key_resolver = KeyResolver()
            key_resolver.put_key(kek)

            # Set the require Encryption, KEK and key resolver on the service object.
            self._service.require_encryption = True
            self._service.key_encryption_key = kek
            self._service.key_resolver_funcion = key_resolver.resolve_key
        pass
     
    def __create__(self, queue) -> bool:
        if (not self._service is None):
            try:
                self._service.create_queue(queue)
                return True
            except AzureException as e:
                log.error('failed to create {} with error {}'.format(queue, e))
                return False
        else:
            return True
        pass

    def register_model(self, storagemodel:object):
        modelname = storagemodel.__class__.__name__     
        if isinstance(storagemodel, StorageQueueModel):
            if (not modelname in self._models):
                self.__create__(storagemodel._queuename)
                self._models.append(modelname)
                log.info('model {} registered successfully. Models are {!s}'.format(modelname, self._models))      
        pass

    def put(self, storagemodel:object) -> StorageQueueModel:
        """ insert queue message into storage """

        modelname = storagemodel.__class__.__name__
        if isinstance(storagemodel, StorageQueueModel):
            if (modelname in self._models):
                """ peek first message in queue """
                try:
                    message = self._service.put_message(storagemodel._queuename, storagemodel.getmessage())
                    storagemodel.mergemessage(message)

                except AzureException as e:
                    log.error('can not save queue message:  queue {} with message {} because {!s}'.format(storagemodel._queuename, storagemodel.content, e))
                    storagemodel = None
            else:
                log.info('please register model {} first'.format(modelname))
                storagemodel = None
        else:
            log.info('model {} is not a Queue Model'.format(modelname))
            storagemodel = None

        return storagemodel

    def peek(self, storagemodel:object) -> StorageQueueModel:
        """ lookup the next message in queue """

        modelname = storagemodel.__class__.__name__
        if isinstance(storagemodel, StorageQueueModel):
            if (modelname in self._models):
                """ peek first message in queue """
                try:
                    messages = self._service.peek_messages(storagemodel._queuename, num_messages=1)

                    """ parse retrieved message """
                    for message in messages:
                        storagemodel.mergemessage(message)

                    """ no message retrieved ?"""
                    if storagemodel.id is None:
                        storagemodel = None

                except AzureException as e:
                    log.error('can not peek queue message:  queue {} with message {} because {!s}'.format(storagemodel._queuename, storagemodel.content, e))
                    storagemodel = None
            else:
                log.info('please register model {} first'.format(modelname))
                storagemodel = None
        else:
            log.info('model {} is not a Queue Model'.format(modelname))
            storagemodel = None

        return storagemodel

    def get(self, storagemodel:object, hide = 0) -> StorageQueueModel:
        """ lookup the next message in queue """
        modelname = storagemodel.__class__.__name__
        if isinstance(storagemodel, StorageQueueModel):
            if (modelname in self._models):
                """ get first message in queue """
                try:
                    if hide > 0:
                        messages = self._service.get_messages(storagemodel._queuename, num_messages=1, visibility_timeout = hide)
                    else:
                        messages = self._service.get_messages(storagemodel._queuename, num_messages=1)
                    
                    """ parse retrieved message """
                    for message in messages:
                        storagemodel.mergemessage(message)

                    """ no message retrieved ?"""
                    if storagemodel.id is None:
                        storagemodel = None

                except AzureException as e:
                    log.error('can not get queue message:  queue {} with message {} because {!s}'.format(storagemodel._queuename, storagemodel.content, e))
                    storagemodel = None
            else:
                log.info('please register model {} first'.format(modelname))
                storagemodel = None
        else:
            log.info('model {} is not a Queue Model'.format(modelname))
            storagemodel = None

        return storagemodel

    def update(self, storagemodel:object, hide = 0) -> StorageQueueModel:
        """ update the message in queue """
        modelname = storagemodel.__class__.__name__
        if isinstance(storagemodel, StorageQueueModel):
            if (modelname in self._models):
                """ check if message in queue """
                if (storagemodel.id != '') and (storagemodel.pop_receipt != '') and (not storagemodel.id is None) and (not storagemodel.pop_receipt is None):
                    try:
                        content = storagemodel.getmessage()
                        message = self._service.update_message(storagemodel._queuename, storagemodel.id, storagemodel.pop_receipt, visibility_timeout = hide, content=content)
                        storagemodel.content = content
                        storagemodel.pop_receipt = message.pop_receipt

                    except AzureException as e:
                        log.error('can not update queue message:  queue {} with message.id {!s} because {!s}'.format(storagemodel._queuename, storagemodel.id, e))
                        storagemodel = None
                else:
                    log.info('cant update queuemessage {} due to missing id and pop_receipt'.format(modelname))
                    storagemodel = None
            else:
                log.info('please register model {} first'.format(modelname))
                storagemodel = None
        else:
            log.info('model {} is not a Queue Model'.format(modelname))
            storagemodel = None

        return storagemodel

    def delete(self, storagemodel:object) -> bool:
        """ delete the message in queue """
        modelname = storagemodel.__class__.__name__
        deleted = False
        if isinstance(storagemodel, StorageQueueModel):
            if (modelname in self._models):
                """ check if message in queue """
                if (storagemodel.id != '') and (storagemodel.pop_receipt != '') and (not storagemodel.id is None) and (not storagemodel.pop_receipt is None):
                    try:
                        self._service.delete_message(storagemodel._queuename, storagemodel.id, storagemodel.pop_receipt)
                        deleted = True

                    except AzureException as e:
                        log.error('can not delete queue message:  queue {} with message.id {!s} because {!s}'.format(storagemodel._queuename, storagemodel.id, e))
                else:
                    log.info('cant update queuemessage {} due to missing id and pop_receipt'.format(modelname))
            else:
                log.info('please register model {} first'.format(modelname))
        else:
            log.info('model {} is not a Queue Model'.format(modelname))

        return deleted
