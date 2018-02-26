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
    _encrypt = False
    
    def __init__(self, **kwargs):                  
        """ constructor """
        super().__init__()
        if self.__class__._queuename == '':
            self._queuename = self.__class__.__name__.lower()
        else:
            self._queuename = self.__class__._queuename
        self._dateformat = self.__class__._dateformat
        self._datetimeformat = self.__class__._datetimeformat
        self._encrypt = self.__class__._encrypt
               
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

    _modeldefinitions = []
    REGISTERED = True

    """ decorators """
    def get_modeldefinition(registered=False):
        def wrap(func):
            @wraps(func)
            def wrapper(self, storagemodel, modeldefinition=None, *args, **kwargs):

                """ modeldefinition already determined """
                if not modeldefinition is None:
                    return func(self, storageobject, modeldefinition, *args, **kwargs)
            
                """ find modeldefinition for StorageQueueModel or StorageQueueModel """
                if isinstance(storagemodel, StorageQueueModel):
                    definitionlist = [definition for definition in self._modeldefinitions if definition['modelname'] == storagemodel.__class__.__name__]
                else:
                    log.info('Argument is not an StorageQueueModel')
                    raise Exception("Argument is not an StorageQueueModel")
                
                if len(definitionlist) == 1:
                    modeldefinition = definitionlist[0]

                elif len(definitionlist) > 1:
                    raise Exception("multiple registration for model")

                if registered and (not isinstance(modeldefinition, dict)):
                    raise Exception("Please register Model first")

                return func(self, storagemodel, modeldefinition, *args, **kwargs)               


            return wrapper
        return wrap

    def __init__(self, **kwargs):

        """ parse kwargs """
        self._account_name = kwargs.get('AZURE_STORAGE_NAME', '')
        self._account_key = kwargs.get('AZURE_STORAGE_KEY', '')
        self._is_emulated = kwargs.get('AZURE_STORAGE_IS_EMULATED', False)
        self._key_identifier = kwargs.get('AZURE_KEY_IDENTIFIER', '')
        self._secret_key = kwargs.get('AZURE_SECRET_KEY', '')

        """ account & service init """
        if self._is_emulated:
            self._account = CloudStorageAccount(is_emulated=True)

        elif self._account_name != '' and self._account_key != '':
            self._account = CloudStorageAccount(self._account_name, self._account_key)

        else:
            raise AzureException

        """ registered models """
        self._modeldefinitions = []
 
    def __create__(self, modeldefinition:dict) -> bool:
        if (not modeldefinition['queueservice'] is None):
            try:
                modeldefinition['queueservice'].create_queue(modeldefinition['queuename'])
                return True

            except AzureException as e:
                log.error('failed to create {} with error {}'.format(modeldefinition['queuename'], e))
                return False
        else:
            return True
        pass

    def __deletequeue__(self, modeldefinition:dict) -> bool:
        if (not modeldefinition['queueservice'] is None):
            try:
                modeldefinition['queueservice'].delete_queue(modeldefinition['queuename'])
                return True

            except AzureException as e:
                log.error('failed to delete {} with error {}'.format(modeldefinition['queuename'], e))
                return False
        else:
            return True
        pass

    @get_modeldefinition()
    def register_model(self, storagemodel:object, modeldefinition = None):
        """ set up an Queueservice for an StorageQueueModel in your  Azure Storage Account
            Will create the Queue if not exist!
        
            required Parameter is:
            - storagemodel: StorageQueueModel(Object)
        """
        if modeldefinition is None: 
                
            """ now register model """
            modeldefinition = {
                'modelname': storagemodel.__class__.__name__,
                'queuename': storagemodel._queuename,
                'encrypt': storagemodel._encrypt,
                'queueservice': self._account.create_queue_service()
                }    

            """ encrypt queue service """
            if modeldefinition['encrypt']:

                # Create the KEK used for encryption.
                # KeyWrapper is the provided sample implementation, but the user may use their own object as long as it implements the interface above.
                kek = KeyWrapper(self._key_identifier, self._secret_key) #  Key identifier

                # Create the key resolver used for decryption.
                # KeyResolver is the provided sample implementation, but the user may use whatever implementation they choose so long as the function set on the service object behaves appropriately.
                key_resolver = KeyResolver()
                key_resolver.put_key(kek)                           

                # Set the require Encryption, KEK and key resolver on the service object.
                modeldefinition['queueservice'].require_encryption = True
                modeldefinition['queueservice'].key_encryption_key = kek
                modeldefinition['queueservice'].key_resolver_funcion = key_resolver.resolve_key
            
            self.__create__(modeldefinition)
                
            self._modeldefinitions.append(modeldefinition)

            log.info('model {} registered successfully. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))
        else:
            log.info('model {} already registered. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))

        pass

    @get_modeldefinition(REGISTERED)
    def unregister_model(self, storagemodel:object, modeldefinition = None,  delete_queue=False):
        """ clear up an Queueservice for an StorageQueueModel in your  Azure Storage Account
            Will delete the hole Queue if delete_queue Flag is True!
        
            required Parameter is:
            - storagemodel: StorageQueueModel(Object)

            Optional Parameter is:
            - delete_queue: bool
        """
        
        """ remove from modeldefinitions """
        for i in range(len(self._modeldefinitions)):
            if self._modeldefinitions[i]['modelname'] == modeldefinition['modelname']:
                del self._modeldefinitions[i]
                break

        """ delete queue from storage if delete_queue == True """        
        if delete_queue:
            self.__deletequeue__(modeldefinition)

        log.info('model {} unregistered successfully. Models are {!s}'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))      
        pass

    @get_modeldefinition(REGISTERED)
    def put(self, storagemodel:object, modeldefinition = None) -> StorageQueueModel:
        """ insert queue message into storage """
        try:
            message = modeldefinition['queueservice'].put_message(storagemodel._queuename, storagemodel.getmessage())
            storagemodel.mergemessage(message)

        except AzureException as e:
            log.error('can not save queue message:  queue {} with message {} because {!s}'.format(storagemodel._queuename, storagemodel.content, e))
            storagemodel = None

        finally:
            return storagemodel

    @get_modeldefinition(REGISTERED)
    def peek(self, storagemodel:object, modeldefinition = None) -> StorageQueueModel:
        """ lookup the next message in queue """

        try:
            messages = modeldefinition['queueservice'].peek_messages(storagemodel._queuename, num_messages=1)

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
    def get(self, storagemodel:object, modeldefinition = None, hide = 0) -> StorageQueueModel:
        """ get the next message in queue """
        try:
            if hide > 0:
                messages = modeldefinition['queueservice'].get_messages(storagemodel._queuename, num_messages=1, visibility_timeout = hide)
            else:
                messages = modeldefinition['queueservice'].get_messages(storagemodel._queuename, num_messages=1)
                    
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
    def update(self, storagemodel:object, modeldefinition = None, hide = 0) -> StorageQueueModel:
        """ update the message in queue """

        if (storagemodel.id != '') and (storagemodel.pop_receipt != '') and (not storagemodel.id is None) and (not storagemodel.pop_receipt is None):
            try:
                content = storagemodel.getmessage()
                message = modeldefinition['queueservice'].update_message(storagemodel._queuename, storagemodel.id, storagemodel.pop_receipt, visibility_timeout = hide, content=content)
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
    def delete(self, storagemodel:object, modeldefinition = None) -> bool:
        """ delete the message in queue """
        deleted = False
        if (storagemodel.id != '') and (storagemodel.pop_receipt != '') and (not storagemodel.id is None) and (not storagemodel.pop_receipt is None):
            try:
                modeldefinition['queueservice'].delete_message(storagemodel._queuename, storagemodel.id, storagemodel.pop_receipt)
                deleted = True

            except AzureException as e:
                log.error('can not delete queue message:  queue {} with message.id {!s} because {!s}'.format(storagemodel._queuename, storagemodel.id, e))
        else:
            log.info('cant update queuemessage {} due to missing id and pop_receipt'.format(modestoragemodel._queuenamelname))

        return deleted
