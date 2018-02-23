""" imports & globals """
from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage import CloudStorageAccount
from azure.storage.queue import QueueService, QueueMessage

import datetime
from ast import literal_eval
from functools import wraps


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
        if self.__class__._queuename == '':
            self._queuename = self.__class__.__name__.lower()
        else:
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
                        if to_type is datetime.datetime:
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
        - AZURE_REQUIRE_ENCRYPTION
        - AZURE_KEY_IDENTIFIER
        - AZURE_SECRET_KEY
        - AZURE_STORAGE_IS_EMULATED
    """

    _account = None
    _account_name = ''
    _account_key = ''
    _is_emulated = False
    _encrypt = False  
    _models = []
    _service = None
    REGISTERED = True

    """ decorators """
    def get_modeldefinition(registered=False):
        def wrap(func):
            @wraps(func)
            def wrapper(self, storagemodel, *args, **kwargs):
            
                """ find modeldefinition for StorageQueueModel or StorageQueueModel """
                if isinstance(storagemodel, StorageQueueModel):
     
                    if registered:
                        modelname = storagemodel.__class__.__name__
                        if modelname in self._models:
                            return func(self, storagemodel, *args, **kwargs)
                        else:
                            log.info('please register model {} first'.format(modelname))
                            raise Exception("Please register Model first")
                    else:
                        return func(self, storagemodel, *args, **kwargs)                
                    
                else:
                    log.info('Argument is not an StorageQueueModel')
                    raise Exception("Argument is not an StorageQueueModel")

            return wrapper
        return wrap

    def __init__(self, **kwargs):

        """ parse kwargs """
        self._account_name = kwargs.get('AZURE_STORAGE_NAME', '')
        self._account_key = kwargs.get('AZURE_STORAGE_KEY', '')
        self._is_emulated = kwargs.get('AZURE_STORAGE_IS_EMULATED', False)
        self._key_identifier = kwargs.get('AZURE_KEY_IDENTIFIER', '')
        self._secret_key = kwargs.get('AZURE_SECRET_KEY', '')
        self._encrypt = kwargs.get('AZURE_REQUIRE_ENCRYPTION', False)

        """ account & service init """
        if self._is_emulated:
            self._account = CloudStorageAccount(is_emulated=True)
            self._service = self._account.create_queue_service()

        elif self._account_name != '' and self._account_key != '':
            self._account = CloudStorageAccount(self._account_name, self._account_key)
            self._service = self._account.create_queue_service()

        else:
            raise AzureException


        """ registered models """
        self._models = []


        """ encrypt queue service """
        if self._encrypt:

            # Create the KEK used for encryption.
            # KeyWrapper is the provided sample implementation, but the user may use their own object as long as it implements the interface above.
            kek = KeyWrapper(self._key_identifier, self._secret_key) #  Key identifier

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

    def __deletequeue__(self, queue) -> bool:
        if (not self._service is None):
            try:
                self._service.delete_queue(queue)
                return True
            except AzureException as e:
                log.error('failed to delete {} with error {}'.format(queue, e))
                return False
        else:
            return True
        pass

    @get_modeldefinition()
    def register_model(self, storagemodel:object):
        modelname = storagemodel.__class__.__name__
        if (not modelname in self._models):
            self.__create__(storagemodel._queuename)
            self._models.append(modelname)
            log.info('model {} registered successfully. Models are {!s}'.format(modelname, self._models))      
        pass

    @get_modeldefinition(REGISTERED)
    def unregister_model(self, storagemodel:object, delete_queue=False):
        """ clear up an Queueservice for an StorageQueueModel in your  Azure Storage Account
            Will delete the hole Queue if delete_queue Flag is True!
        
            required Parameter is:
            - storagemodel: StorageQueueModel(Object)

            Optional Parameter is:
            - delete_queue: bool
        """
        modelname = storagemodel.__class__.__name__
        
        """ remove from modeldefinitions """
        for i in range(len(self._models)):
            if self._models[i] == modelname:
                del self._models[i]
                break

        """ delete queue from storage if delete_queue == True """        
        if delete_queue:
            self.__deletequeue__(storagemodel._queuename)

        log.info('model {} registered successfully. Models are {!s}'.format(modelname, self._models))      
        pass

    @get_modeldefinition(REGISTERED)
    def put(self, storagemodel:object) -> StorageQueueModel:
        """ insert queue message into storage """
        try:
            message = self._service.put_message(storagemodel._queuename, storagemodel.getmessage())
            storagemodel.mergemessage(message)

        except AzureException as e:
            log.error('can not save queue message:  queue {} with message {} because {!s}'.format(storagemodel._queuename, storagemodel.content, e))
            storagemodel = None

        finally:
            return storagemodel

    @get_modeldefinition(REGISTERED)
    def peek(self, storagemodel:object) -> StorageQueueModel:
        """ lookup the next message in queue """

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

        finally:
            return storagemodel

    @get_modeldefinition(REGISTERED)
    def get(self, storagemodel:object, hide = 0) -> StorageQueueModel:
        """ get the next message in queue """
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

        finally:
            return storagemodel

    @get_modeldefinition(REGISTERED)
    def update(self, storagemodel:object, hide = 0) -> StorageQueueModel:
        """ update the message in queue """

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

        return storagemodel

    @get_modeldefinition(REGISTERED)
    def delete(self, storagemodel:object) -> bool:
        """ delete the message in queue """
        deleted = False
        if (storagemodel.id != '') and (storagemodel.pop_receipt != '') and (not storagemodel.id is None) and (not storagemodel.pop_receipt is None):
            try:
                self._service.delete_message(storagemodel._queuename, storagemodel.id, storagemodel.pop_receipt)
                deleted = True

            except AzureException as e:
                log.error('can not delete queue message:  queue {} with message.id {!s} because {!s}'.format(storagemodel._queuename, storagemodel.id, e))
        else:
            log.info('cant update queuemessage {} due to missing id and pop_receipt'.format(modelname))

        return deleted
