""" imports & globals """
from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage import CloudStorageAccount
from azure.storage.blob import BlockBlobService, BlobBlock

import datetime, os
from ast import literal_eval
from functools import wraps
import uuid


""" helpers """
from azurestoragewrap.snippets import safe_cast, test_azurestorage_nameconventions

""" encryption """
from azurestoragewrap.encryption import (
    KeyWrapper,
    KeyResolver    
    )

""" custom Exceptions """
from azurestoragewrap.exception import  AzureStorageWrapException, NameConventionError, ModelNotRegisteredError, ModelRegisteredMoreThanOnceError

""" logging """
import logging
log = logging.getLogger('azurestoragewrap')


""" model base classes """
class StorageBlobModel(BlobBlock):
    _containername = ''
    _encrypt = False
    _dateformat = ''
    _datetimeformat = ''
    __blobtype__ = None
    __blobname__ = ''
    __blobsource__ = None


    def __init__(self, source=None, blobname=None, **kwargs):                  
        """ Create a StorageBlobModel Instance from a given path/url. Parameters are:            
            - source (required) = a text or a filepath
            - blobname (optional) = Name of the blob """

        """ determine blob configuration """
        if self.__class__._containername == '':
            self._containername = self.__class__.__name__.lower()
        else:
            self._containername = self.__class__._containername

        self._encrypt = self.__class__._encrypt
        self.exists = None
               
        """ generate a uuid as blobname: if parameter blobname is None generate a UUID as blobname """
        if blobname is None:
            self.__blobname__ = str(uuid.uuid4())
        else:
            self.__blobname__ = blobname

        """ determine Blob Type and Source"""
        try:
            self.__blobsource__ = source

            if self.__blobsource__ is None:
                self.__blobtype__ = None

            elif os.path.isfile(self.__blobsource__):
                self.__blobtype__ = 'FILE'

            elif isinstance(self.__blobsource__, str): 
                self.__blobtype__ = 'TEXT'

        except Exception as e:
            raise AzureStorageWrapException(self, e)

        """ collect metadata from **kwargs """
        self.__setblobmetadata__(kwargs)

        """ super """
        super().__init__()
          

    def getblobtype(self):
        return self.__blobtype__

    def getblobname(self):
        return self.__blobname__

    def getblobsource(self):
        return self.__blobsource__

    def getblobmetadata(self):
        """ parse self into unicode string as message content """   
        image = {}
        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key !='':                                      
                image[key] = getattr(self, key, default)                                  
        return dict(image)

    def __setblobmetadata__(self, kwargs):
        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '':
                if (not key in vars(BlobBlock).items()):
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

    def exists(self):
        return self._exists



""" wrapper classes """
class StorageBlobContext():
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
                if isinstance(storagemodel, StorageBlobModel):
                    definitionlist = [definition for definition in self._modeldefinitions if definition['modelname'] == storagemodel.__class__.__name__]
                else:
                    log.info('Argument is not an StorageBlobModel')
                    raise AzureStorageWrapException(storagemodel, "Argument is not an StorageBlobModel")
                
                if len(definitionlist) == 1:
                    modeldefinition = definitionlist[0]

                elif len(definitionlist) > 1:
                    raise ModelRegisteredMoreThanOnceError(storagemodel)

                if registered and (not isinstance(modeldefinition, dict)):
                    raise ModelNotRegisteredError(storagemodel)

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
        if (not modeldefinition['blobservice'] is None):
            try:
                modeldefinition['blobservice'].create_container(modeldefinition['container'])
                return True

            except Exception as e:
                msg = 'failed to create {} with error {}'.format(modeldefinition['container'], e)
                raise AzureStorageWrapException(msg=msg)

        else:
            return True
        pass

    def __delete__(self, modeldefinition:dict) -> bool:
        if (not modeldefinition['blobservice'] is None):
            try:
                modeldefinition['blobservice'].delete_container(modeldefinition['container'])
                return True

            except Exception as e:
                msg = 'failed to delete {} with error {}'.format(modeldefinition['container'], e)
                raise AzureStorageWrapException(msg=msg)

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
            
            """ test if containername already exists """
            if [model for model in self._modeldefinitions if model['container'] == storagemodel._containername]:
                raise NameConventionError(storagemodel._containername)

            """ test if containername fits to azure naming rules """
            if not test_azurestorage_nameconventions(storagemodel._containername, 'StorageBlobModel'):
                raise NameConventionError(storagemodel._containername)
             
            """ now register model """
            modeldefinition = {
                'modelname': storagemodel.__class__.__name__,
                'container': storagemodel._containername,
                'encrypt': storagemodel._encrypt,
                'blobservice': self._account.create_block_blob_service()
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
                modeldefinition['blobservice'].require_encryption = True
                modeldefinition['blobservice'].key_encryption_key = kek
                modeldefinition['blobservice'].key_resolver_funcion = key_resolver.resolve_key
            
            self.__create__(modeldefinition)
                
            self._modeldefinitions.append(modeldefinition)

            log.info('model {} registered successfully. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))
        else:
            log.info('model {} already registered. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))

        pass

    @get_modeldefinition(REGISTERED)
    def unregister_model(self, storagemodel:object, modeldefinition = None,  delete_blob=False):
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
        if delete_blob:
            self.__delete__(modeldefinition)

        log.info('model {} unregistered successfully. Models are {!s}'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))      
        pass

    @get_modeldefinition(REGISTERED)
    def upload(self, storagemodel:object, modeldefinition = None):
        """ insert blob message into storage """
        try:
            if storagemodel.getblobtype() == 'FILE':
                """ upload file """
                modeldefinition['blobservice'].create_blob_from_path(modeldefinition['container'], storagemodel.getblobname(), storagemodel.getblobsource(), metadata=storagemodel.getblobmetadata())
                storagemodel._exists = True

            elif storagemodel.getblobtype() == 'TEXT':
                modeldefinition['blobservice'].create_blob_from_text(modeldefinition['container'], storagemodel.getblobname(), storagemodel.getblobsource(), metadata=storagemodel.getblobmetadata())
                storagemodel._exists = True
            
            else:
                raise AzureStorageWrapException(storagemodel, "StorageBlobModel does not contain content") 


        except Exception as e:
            msg = 'can not save blob in container {} because {!s}'.format(storagemodel._containername, e)
            raise AzureStorageWrapException(storagemodel, msg=msg)
           
        finally:
            return storagemodel

    @get_modeldefinition(REGISTERED)
    def download(self, storagemodel:object, modeldefinition = None):
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

        except Exception as e:
            storagemodel = None
            msg = 'can not peek queue message:  queue {} with message {} because {!s}'.format(storagemodel._queuename, storagemodel.content, e)
            raise AzureStorageWrapException(msg=msg)

        finally:
            return storagemodel

    @get_modeldefinition(REGISTERED)
    def delete(self, storagemodel:object, modeldefinition = None) -> bool:
        """ delete the message in queue """
        deleted = False
        if (storagemodel.id != '') and (storagemodel.pop_receipt != '') and (not storagemodel.id is None) and (not storagemodel.pop_receipt is None):
            try:
                modeldefinition['queueservice'].delete_message(storagemodel._queuename, storagemodel.id, storagemodel.pop_receipt)
                deleted = True

            except Exception as e:
                msg = 'can not delete queue message:  queue {} with message.id {!s} because {!s}'.format(storagemodel._queuename, storagemodel.id, e)
                raise AzureStorageWrapException(msg=msg)

        else:
            log.info('cant update queuemessage {} due to missing id and pop_receipt'.format(modestoragemodel._queuenamelname))

        return deleted

    @get_modeldefinition(REGISTERED)
    def list(self, storagemodel:object, modeldefinition = None) ->list:
        """ list blob messages in container """
        try:
            blobnames = []

            generator = modeldefinition['blobservice'].list_blobs(modeldefinition['container'])
            for blob in generator:
                blobnames.append(blob.name)

        except Exception as e:
            msg = 'can not list blobs in container {} because {!s}'.format(storagemodel._containername, e)
            raise AzureStorageWrapException(storagemodel, msg=msg)
           
        finally:
            return blobnames
