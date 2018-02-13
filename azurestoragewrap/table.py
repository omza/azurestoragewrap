""" imports & globals """
from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage.table import TableService, Entity

import datetime

""" snippets """
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
class StorageTableModel(object):
    _tablename = ''
    _dateformat = ''
    _datetimeformat = ''
    _exists = None
    _encryptedproperties = []

    PartitionKey = ''
    RowKey = ''


    def __init__(self, **kwargs):                  
        """ constructor """
        
        self._tablename = self.__class__._tablename
        self._dateformat = self.__class__._dateformat
        self._datetimeformat = self.__class__._datetimeformat
        self._exists = None
               
        """ parse **kwargs into instance var """
        self.PartitionKey = kwargs.get('PartitionKey', '')
        self.RowKey = kwargs.get('RowKey', '')

        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '':
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

        """ set primary keys from data"""
        if self.PartitionKey == '':
            self.__setPartitionKey__()

        if self.RowKey == '':
            self.__setRowKey__()

        """ initialize collectionobjects """
        self.__setCollections__()

        """ define properties to be encrypted """
        self.__setEncryptedProperties__()

        pass

         
    def __setPartitionKey__(self):
        """ parse storage primaries from instance attribute 
            overwrite if inherit this class
        """
        pass

    def __setRowKey__(self):
        """ parse storage primaries from instance attribute 
            overwrite if inherit this class
        """
        pass

    def __setCollections__(self):
        """ parse storage primaries from instance attribute 
            overwrite if inherit this class
        """
        pass

    def __setEncryptedProperties__(self):
        """ give back a list of property names to be encrypted client side
            default: all properties are not encrypted
            overwrite if inherit this class
        """
        pass


    def dict(self) -> dict:        
        """ parse self into dictionary """
     
        image = {}

        for key, value in vars(self).items():
            if not key.startswith('_') and key !='':
                if key in ['PartitionKey', 'RowKey']:
                    image[key] = str(value)
                else:
                    image[key] = value
        
        return image

    def entity(self) -> dict:        
        """ parse self into dictionary """
     
        image = {}

        for key, value in vars(self).items():
            if not key.startswith('_') and key !='':
                if type(value) in [str, int, bool, datetime.date, datetime.datetime]:
                    if key in ['PartitionKey', 'RowKey']:
                        image[key] = str(value)
                    else:
                        image[key] = value                   
        
        return image

class StorageTableCollection(list):
    _tablename = ''
    _filter = ''

    def __init__(self, tablename='', filter='*'):
        """ constructor """

        """ query configuration """
        self._tablename = tablename if tablename != '' else self.__class__._tablename
        self._filter = filter        
        pass

    def find(self, key, value) -> dict:
        pass

    def filter(self, key, values):

        resultset = [item for item in self if item[key] in values]
        self.clear()
        self.extend(resultset)

    pass

""" wrapper classes """
class StorageTableContext():
    """Initializes the repository with the specified settings dict.
        Required settings in config dict are:
        - AZURE_STORAGE_NAME
        - STORAGE_KEY
    """
    
    _models = []
    _encryptproperties = False
    _encrypted_properties = []
    _tableservice = None
    _storage_key = ''
    _storage_name = ''

    def __init__(self, **kwargs):

        self._storage_name = kwargs.get('AZURE_STORAGE_NAME', '')
        self._storage_key = kwargs.get('AZURE_STORAGE_KEY', '')

        """ service init """
        self._models = []
        if self._storage_key != '' and self._storage_name != '':
            self._tableservice = TableService(account_name = self._storage_name, account_key = self._storage_key, protocol='https')

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
            self._encryptproperties = True
            self._tableservice.key_encryption_key = kek
            self._tableservice.key_resolver_funcion = key_resolver.resolve_key
            self._tableservice.encryption_resolver_function = self.__encryptionresolver__


        pass

    def __createtable__(self, tablename) -> bool:
        if (not self._tableservice is None):
            try:
                self._tableservice.create_table(tablename)
                return True
            except AzureException as e:
                log.error('failed to create {} with error {}'.format(tablename, e))
                return False
        else:
            return True
        pass

    # Define the encryption resolver_function.
    def __encryptionresolver__(self, pk, rk, property_name):
        if property_name in self._encrypted_properties:
            return True
            #log.debug('encrypt field {}'.format(property_name))
        
        #log.debug('dont encrypt field {}'.format(property_name))
        return False

    def register_model(self, storagemodel:object):
        modelname = storagemodel.__class__.__name__     
        if isinstance(storagemodel, StorageTableModel):
            if (not modelname in self._models):
                self.__createtable__(storagemodel._tablename)
                self._models.append(modelname)

                """ set properties to be encrypted client side """
                if self._encryptproperties:
                    self._encrypted_properties += storagemodel._encryptedproperties

                log.info('model {} registered successfully. Models are {!s}. Encrypted fields are {!s} '.format(modelname, self._models, self._encrypted_properties))      
        pass

    def table_isempty(self, tablename, PartitionKey='', RowKey = '') -> bool:
        if  (not self._tableservice is None):

            filter = "PartitionKey eq '{}'".format(PartitionKey) if PartitionKey != '' else ''
            if filter == '':
                filter = "RowKey eq '{}'".format(RowKey) if RowKey != '' else ''
            else:
                filter = filter + ("and RowKey eq '{}'".format(RowKey) if RowKey != '' else '')
            try:
                entities = list(self._tableservice.query_entities(tablename, filter = filter, select='PartitionKey', num_results=1))
                if len(entities) == 1: 
                    return False
                else:
                    return True

            except AzureMissingResourceHttpError as e:
                log.debug('failed to query {} with error {}'.format(tablename, e))
                return True

        else:
            return True
        pass

    def exists(self, storagemodel) -> bool:
        exists = False
        if isinstance(storagemodel, StorageTableModel):
            modelname = storagemodel.__class__.__name__
            if (modelname in self._models):
                if storagemodel._exists is None:
                    try:
                        entity = self._tableservice.get_entity(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey)
                        storagemodel._exists = True
                        exists = True
            
                    except AzureMissingResourceHttpError:
                        storagemodel._exists = False
                else:
                    exists = storagemodel._exists
            else:
                log.debug('please register model {} first'.format(modelname))
                        
        return exists       

    def get(self, storagemodel) -> StorageTableModel:
        """ load entity data from storage to vars in self """

        if isinstance(storagemodel, StorageTableModel):
            modelname = storagemodel.__class__.__name__
            if (modelname in self._models):
                try:
                    entity = self._tableservice.get_entity(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey)
                    storagemodel._exists = True
        
                    """ sync with entity values """
                    for key, default in vars(storagemodel).items():
                        if not key.startswith('_') and key not in ['','PartitionKey','RowKey']:
                            value = getattr(entity, key, None)
                            if not value is None:
                                setattr(storagemodel, key, value)
             
                except AzureMissingResourceHttpError as e:
                    log.debug('can not get table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey, e))
                    storagemodel._exists = False

                except Exception as e:
                    log.debug('can not get table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey, e))
                    storagemodel._exists = False

            else:
                log.debug('please register model {} first to {!s}'.format(modelname, self._models))

            return storagemodel

        else:
            return None

    def insert(self, storagemodel) -> StorageTableModel:
        """ insert model into storage """
        if isinstance(storagemodel, StorageTableModel):
            modelname = storagemodel.__class__.__name__
            if (modelname in self._models):
                try:            
                    self._tableservice.insert_or_replace_entity(storagemodel._tablename, storagemodel.entity())
                    storagemodel._exists = True

                except AzureMissingResourceHttpError as e:
                    log.debug('can not insert or replace table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey, e))
            else:
                log.debug('please register model {} first'.format(modelname))

            return storagemodel
        else:
            return None

    def merge(self, storagemodel) -> StorageTableModel:
        """ try to merge entry """
        if isinstance(storagemodel, StorageTableModel):
            modelname = storagemodel.__class__.__name__
            if (modelname in self._models):
                try:            
                    self._tableservice.insert_or_merge_entity(storagemodel._tablename, storagemodel.entity())
                    storagemodel._exists = True

                except AzureMissingResourceHttpError as e:
                    log.debug('can not insert or merge table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey, e))
            else:
                log.debug('please register model {} first'.format(modelname))

            return storagemodel
        else:
            return None
    
    def delete(self,storagemodel):
        """ delete existing Entity """
        if isinstance(storagemodel, StorageTableModel):
            modelname = storagemodel.__class__.__name__
            if (modelname in self._models):
                try:
                    self._tableservice.delete_entity(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey)
                    storagemodel._exists = False

                except AzureMissingResourceHttpError as e:
                    log.debug('can not delete table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(storagemodel._tablename, storagemodel.PartitionKey, storagemodel.RowKey, e))

            else:
                log.debug('please register model {} first'.format(modelname))

            return storagemodel
        else:
            return None


    def __changeprimarykeys__(self, PartitionKey = '', RowKey = ''):
        """ Change Entity Primary Keys into new instance:

            - PartitionKey and/or
            - RowKey
        """

        PartitionKey = PartitionKey if PartitionKey != '' else self._PartitionKey
        RowKey = RowKey if RowKey != '' else self._RowKey

        """ change Primary Keys if different to existing ones """
        if (PartitionKey != self._PartitionKey) or (RowKey != self._RowKey):
            return True, PartitionKey, RowKey
        else:
            return False, PartitionKey, RowKey
        pass
            
    def moveto(self, PartitionKey = '', RowKey = ''):
        """ Change Entity Primary Keys and move in Storage:

            - PartitionKey and/or
            - RowKey
        """
        changed, PartitionKey, RowKey = self.__changeprimarykeys__(PartitionKey, RowKey)

        if changed:

            """ sync self """
            new = self.copyto(PartitionKey, RowKey)
            new.save()

            """ delete Entity if exists in Storage """
            self.delete()

    def copyto(self, PartitionKey = '', RowKey = '') -> object:
        """ Change Entity Primary Keys and copy to new Instance:

            - PartitionKey and/or
            - RowKey
        """
        changed, PartitionKey, RowKey = self.__changeprimarykeys__(PartitionKey, RowKey)

        self.load()
        new = self
        new._PartitionKey = PartitionKey
        new._RowKey = RowKey
        new.load()

        return new

    def query(self, storagecollection) -> StorageTableCollection:
        if isinstance(storagecollection, StorageTableCollection):
            try:
                storagecollection.extend(self._tableservice.query_entities(storagecollection._tablename,storagecollection._filter))

            except AzureMissingResourceHttpError as e:
                log.debug('can not query table {} with filters {} because {!s}'.format(storagecollection._tablename, storagecollection._filter, e))            

            return storagecollection
        else:
            return None
