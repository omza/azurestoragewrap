""" imports & globals """
from azure.storage import CloudStorageAccount
from azure.storage.blob import BlockBlobService, Blob, ContentSettings

import datetime, os
from ast import literal_eval
from functools import wraps
import uuid
from mimetypes import guess_type, guess_extension, guess_all_extensions


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
class StorageBlobModel(Blob):
    _containername = ''
    _encrypt = False
    _dateformat = ''
    _datetimeformat = ''

    def __init__(self, **kwargs):                  
        """ Create a StorageBlobModel Instance """

        """ super """
        super().__init__()

        """ determine blob configuration """
        if self.__class__._containername == '':
            self._containername = self.__class__.__name__.lower()
        else:
            self._containername = self.__class__._containername

        self._encrypt = self.__class__._encrypt
               
        """ generate a uuid as blobname: if parameter blobname is None generate a UUID as blobname """
        name = kwargs.get('name', None)
        if name is None:
            self.name = str(uuid.uuid4()).replace('-', '')
        else:
            self.name = str(name)

        """ collect metadata from **kwargs """
        metadataimage = {}
        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '' and (not key in vars(Blob).items()):
                if key in kwargs:                   
                    value = kwargs.get(key)
                    to_type = type(default)              
                    if to_type is datetime.datetime:
                        value = safe_cast(value, to_type, default, self._datetimeformat)
                    elif to_type is datetime.date:
                        value =  safe_cast(value, to_type, default, self._dateformat)
                    else:
                        value = safe_cast(value, to_type, default)
                    
                    setattr(self, key, value)
                    metadataimage[key] = value

                else:
                    setattr(self, key, default)
                    metadataimage[key] = default

        # blob source filename
        self.filename = ''
        metadataimage['filename'] = ''

        # init metadata
        self.metadata = metadataimage

    def __instance_to_metadata__(self):
        """ parse self self.metadata """   
        image = {}
        image['filename'] = self.filename 
        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key !='' and (not key in vars(Blob).items()):                                      
                image[key] = getattr(self, key, default)
               
        self.metadata = image

    def __mergeblob__(self, message):
        """ parse Blob Instance in Model vars """
        if isinstance(message, Blob):
            """ merge queue message vars """
            for key, value in vars(message).items():
                setattr(self, key, value)
                if key == 'metadata':
                    for metakey, metavalue in value.items():
                        if metakey in vars(self):
                            setattr(self, metakey, metavalue)

    def fromfile(self, path_to_file, mimetype=None):
        """ 
        load blob content from file in StorageBlobModel instance. Parameters are:
        - path_to_file (required): path to a local file 
        - mimetype (optional): set a mimetype. azurestoragewrap will guess it if not given 
        """

        if os.path.isfile(path_to_file):

            # Load file into self.__content__
            self.filename = os.path.basename(path_to_file)
            with open(path_to_file, "rb") as in_file:
                self.content = in_file.read()

            #guess mime-type
            self.properties.content_settings = ContentSettings()
            
            if mimetype is None:
                mimetype = guess_type(path_to_file)
                if mimetype[0] is None:
                    mimetype = 'application/octet-stream'
                else: 
                    if not mimetype[1] is None:
                        self.properties.content_settings.content_encoding = mimetype[1]
                    mimetype = mimetype[0]

            self.properties.content_settings.content_type = mimetype

        else:
            raise AzureStorageWrapException(self, 'Can not load blob content, because given path is not a local file')            

    def fromtext(self, text, encoding='utf-8', mimetype='text/plain'):
        """ 
        set blob content from given text in StorageBlobModel instance. Parameters are:
        - text (required): path to a local file 
        - encoding (optional): text encoding (default is utf-8)
        - mimetype (optional): set a mimetype. azurestoragewrap will guess it if not given 
        """
        if isinstance(text, str):
            text = text.encode(encoding, 'ignore')

            # Load text into self.__content__
            self.content = bytes(text)

            self.properties.content_settings = ContentSettings(content_type=mimetype, content_encoding=encoding)

        else:
            raise AzureStorageWrapException(self, 'Can not load blob content, because given text is not from type string')

    def tofile(self, path_to_file, replace_file=False):
        """ 
        save blob content from StorageBlobModel instance to file in given path/file. Parameters are:
        - path_to_file (required): local path or file
        """

        # create full path
        if os.path.isdir(path_to_file):
            if self.filename != '':
                path_to_file = os.path.join(path_to_file, self.filename)
            
            else:
                # guess extention from mimetype
                path_to_file = os.path.join(path_to_file, self.name + guess_extension(self.properties.content_settings.content_type))

        elif os.path.isfile(path_to_file):
            # check if given file extention fits to self.filename or mime type
            #  
            if self.filename != '':
                if os.path.splitext(self.filename)[1] != os.path.splitext(path_to_file)[1]:
                    raise AzureStorageWrapException(self, 'can not save blob to file because file extention {!s} does not fit to source file or mime type'.format(path_to_file))
      
            else:
                mimetype = guess_type(path_to_file)[0]
                if mimtype != self.properties.content_settings.content_type:
                    raise AzureStorageWrapException(self, 'can not save blob to file because file extention {!s} does not fit to source file or mime type'.format(path_to_file))
                
        else:
            raise AzureStorageWrapException(self, 'can not save blob to file because {!s} is not a dir nor a file'.format(path_to_file))
        
        # check if file exists (and replace or error)
        if os.path.isfile(path_to_file):
            if replace_file:
                os.remove(path_to_file)
            else:
                raise AzureStorageWrapException(self, 'can not save blob to file {!s} because file exists and replace_file is False'.format(path_to_file))

        # save file into self.__content__
        self.filename = os.path.basename(path_to_file)
        with open(path_to_file, "wb") as out_file:
            out_file.write(self.content)

        return path_to_file

    def totext(self) ->str:
        """ 
        return blob content from StorageBlobModel instance to a string. Parameters are:
        """
        sreturn = ''
        if self.properties.content_settings.content_encoding is None:
            raise AzureStorageWrapException(self, 'can not convert blob {!s} to text because content_encoding is not given'.format(self.name))
        else:
            sreturn = self.content.decode(self.properties.content_settings.content_encoding, 'ignore')

        return sreturn







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

        if (storagemodel.content is None) or (storagemodel.properties.content_settings.content_type is None):
            # No content to upload
            raise AzureStorageWrapException(storagemodel, "StorageBlobModel does not contain content nor content settings")

        else:
            try:
                # refresh metadata
                storagemodel.__instance_to_metadata__()


                """ upload bytes """
                modeldefinition['blobservice'].create_blob_from_bytes(
                        container_name=modeldefinition['container'], 
                        blob_name=storagemodel.name, 
                        blob=storagemodel.content, 
                        metadata=storagemodel.metadata, 
                        content_settings=storagemodel.properties.content_settings
                    )

                storagemodel._exists = True
                 
            except Exception as e:
                msg = 'can not save blob in container {} because {!s}'.format(storagemodel._containername, e)
                raise AzureStorageWrapException(storagemodel, msg=msg)
           
        return storagemodel

    @get_modeldefinition(REGISTERED)
    def download(self, storagemodel:object, modeldefinition = None):
        """ load blob from storage into StorageBlobModelInstance """

        if (storagemodel.name is None):
            # No content to download
            raise AzureStorageWrapException(storagemodel, "StorageBlobModel does not contain content nor content settings")

        else:
            container_name = modeldefinition['container']
            blob_name = storagemodel.name
            try:
                if modeldefinition['blobservice'].exists(container_name, blob_name):
                    """ download blob """
                    blob = modeldefinition['blobservice'].get_blob_to_bytes(
                            container_name=modeldefinition['container'], 
                            blob_name=storagemodel.name
                        )

                    storagemodel.__mergeblob__(blob)
                 
            except Exception as e:
                msg = 'can not load blob from container {} because {!s}'.format(storagemodel._containername, e)
                raise AzureStorageWrapException(storagemodel, msg=msg)
           
        return storagemodel

    @get_modeldefinition(REGISTERED)
    def delete(self, storagemodel:object, modeldefinition = None) -> bool:
        """ delete the message in queue """
        deleted = False

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
