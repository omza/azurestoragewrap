""" imports & globals """
from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage import CloudStorageAccount
from azure.storage.table import TableService, Entity


import datetime
from functools import wraps

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
    _encrypt = False

    _dateformat = ''
    _datetimeformat = ''
    _exists = None

    PartitionKey = ''
    RowKey = ''

    def __init__(self, **kwargs):                  
        """ constructor """
        
        self._tablename = self.__class__._tablename
        if self._tablename == '':
            self._tablename = self.__class__.__name__
        self._dateformat = self.__class__._dateformat
        self._datetimeformat = self.__class__._datetimeformat
        self._exists = None
        self._encrypt = self.__class__._encrypt
        IncludeRelationship = []
               
        """ parse **kwargs into instance var """
        self.PartitionKey = kwargs.get('PartitionKey', '')
        self.RowKey = kwargs.get('RowKey', '')

        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '':
                to_type = type(default)
                
                if (key in kwargs):       
                    value = kwargs.get(key)
                
                    if to_type is StorageTableQuery:
                        setattr(self, key, value)

                    elif to_type is datetime.datetime:
                        setattr(self, key, safe_cast(value, to_type, default, self._datetimeformat))

                    elif to_type is datetime.date:
                        setattr(self, key, safe_cast(value, to_type, default, self._dateformat))

                    else:
                        setattr(self, key, safe_cast(value, to_type, default))
                
                elif to_type is StorageTableQuery:
                    IncludeRelationship.append({'key': key, 
                                                'query': default})

                else:
                    setattr(self, key, default)

        """ set primary keys from data"""
        if self.PartitionKey == '':
            self.__setPartitionKey__()

        if self.RowKey == '':
            self.__setRowKey__()

        """ initialize Relationship/ related Query Objects """
        for item in IncludeRelationship:
            key = item['key']
            query = item['query']
            pkwhere = getattr(self, query._pkforeignkey, '*')
            rkwhere = getattr(self, query._rkforeignkey, '*') 
            setattr(self, key, StorageTableQuery(query._storagemodel,
                                                 query._pkcondition,
                                                 pkwhere,
                                                 query._rkcondition,
                                                 rkwhere,
                                                 query._select))
            pass

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


    # Define the encryption resolver_function.
    @staticmethod
    def __encryptionresolver__(pk, rk, property_name):
        """ define properties to encrypt 
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

class StorageTableQuery(list):
    """ Initialized a query your azure storage to implement a model relationship by Partition- and/or RowKey

        Required Parameters are:
        - storagemodel: StorageTableModel (Object)
        - pkfilter: str (where clause for PartitionKey)
        - rkfilter: str (where clause for RowKeyFilter)
        - Select: list (define a subset of fields to query) 

    """
    _storagemodel = None
    _select = None
    _queryfilter = ''
    _pkcondition = ''
    _rkcondition = ''
    _pkforeignkey= ''
    _rkforeignkey= ''    

    def __init__(self, storagemodel=None, pkcondition='', pkforeignkey = '', rkcondition = '', rkforeignkey='', select = None):

        """ set storagemodel """
        self._storagemodel = storagemodel
        
        """ set select """
        if (not self._select is None) and (self._select != '') and (not self._storagemodel is None):
            """ parse select statement into self._select """
            for key, value in vars(self).items():
                if not key.startswith('_') and key !='':
                    if type(value) in [str, int, bool, datetime.date, datetime.datetime]:
                        if select.find(key) >= 0:
                            if (not self._select is None) or (self._select != ''):
                                self._select += ', '
                            self._select += key
        else:
            self._select = None
           
        """ query configuration """
        self._pkcondition = pkcondition
        self._rkcondition = rkcondition
        self._pkforeignkey = pkforeignkey         
        self._rkforeignkey = rkforeignkey   
        self._queryfilter = ''
        
        """ determine query filter """
        if self._pkcondition != '' and self._pkforeignkey !='':
            self._queryfilter = "PartitionKey {!s} '{!s}'".format(self._pkcondition, self._pkforeignkey)

        if self._rkcondition != '' and self._rkforeignkey !='':
            if self._queryfilter != '':
                self._queryfilter += ' and '

            self._queryfilter += "RowKey {!s} '{!s}'".format(self._rkcondition, self._rkforeignkey)
        pass

    def find(self, key, condition) -> list:
        """ find a subset of Entities that fits to condition
        
            ! to be done !
        """
        pass

    def filter(self, key, values) -> list:
        resultset = [item for item in self if item[key] in values]
        self.clear()
        self.extend(resultset)
    pass


""" wrapper classes """
class StorageTableContext():
    """Initializes the repository with the specified settings dict.
        Required settings in config dict are:
        - AZURE_STORAGE_NAME
        - AZURE_STORAGE_KEY
        - AZURE_KEY_IDENTIFIER
        - AZURE_SECRET_KEY
        - AZURE_STORAGE_IS_EMULATED
    """
    
    
    _account = None
    _account_name = ''
    _account_key = ''
    _is_emulated = False
    _kek = None
    _key_resolver = None

    _modeldefinitions = []
    REQUIRED = True

    """ decorators """
    def get_modeldefinition(required=False):
        def wrap(func):
            @wraps(func)
            def wrapper(self, storageobject, modeldefinition=None, *args, **kwargs):

                """ modeldefinition already determined """
                if not modeldefinition is None:
                    return func(self, storageobject, modeldefinition, *args, **kwargs)
            
                """ find modeldefinition for StorageTableModel or StorageTableQuery """
                if isinstance(storageobject, StorageTableModel):
                    definitionlist = [definition for definition in self._modeldefinitions if definition['modelname'] == storageobject.__class__.__name__]
     
                elif isinstance(storageobject, StorageTableQuery):
                    """ StorageTableQuery """
                    storagemodel = storageobject._storagemodel
                    definitionlist = [definition for definition in self._modeldefinitions if definition['modelname'] == storagemodel.__class__.__name__]
                else:
                    raise Exception("Argument is not an StorageTableModel nor an StorageTableQuery")


                if len(definitionlist) == 1:
                    modeldefinition = definitionlist[0]

                elif len(definitionlist) > 1:
                    raise Exception("multiple registration for model")

                if required and (not isinstance(modeldefinition, dict)):
                    raise Exception("Please register Model first")

                return func(self, storageobject, modeldefinition, *args, **kwargs)

            return wrapper
        return wrap

    def __init__(self, **kwargs):
        """ parse kwargs """
        self._account_name = kwargs.get('AZURE_STORAGE_NAME', '')
        self._account_key = kwargs.get('AZURE_STORAGE_KEY', '')
        self._is_emulated = kwargs.get('AZURE_STORAGE_IS_EMULATED', False)
        self._key_identifier = kwargs.get('AZURE_KEY_IDENTIFIER', '')
        self._secret_key = kwargs.get('AZURE_SECRET_KEY', '')

        """ account init """
        if self._is_emulated:
            self._account = CloudStorageAccount(is_emulated=True)

        elif self._account_name != '' and self._account_key != '':
            self._account = CloudStorageAccount(self._account_name, self._account_key)
        
        else:
            raise AzureException

        """ init table model list """
        self._modeldefinitions = []

    def __createtable__(self, modeldefinition:dict) -> bool:

        if (not modeldefinition['tableservice'] is None):
            try:
                modeldefinition['tableservice'].create_table(modeldefinition['tablename'])
                return True
            
            except AzureException as e:
                log.error('failed to create {} with error {}'.format(tablename, e))
                return False
        else:
            return False
        pass

    def __deletetable__(self, modeldefinition:dict) -> bool:
        if (not modeldefinition['tableservice'] is None):
            try:
                modeldefinition['tableservice'].delete_table(modeldefinition['tablename'])
                return True
            
            except AzureException as e:
                log.error('failed to create {} with error {}'.format(tablename, e))
                return False
        else:
            return False
        pass

    @get_modeldefinition()
    def register_model(self, storagemodel:object, modeldefinition = None):
        """ set up an Tableservice for an StorageTableModel in your  Azure Storage Account
            Will create the Table if not exist!
        
            required Parameter is:
            - storagemodel: StorageTableModel(Object)

        """
        if modeldefinition is None: 
                
            """ now register model """
            modeldefinition = {
                'modelname': storagemodel.__class__.__name__,
                'tablename': storagemodel._tablename,
                'encrypt': storagemodel._encrypt,
                'tableservice': self._account.create_table_service()
                }

            if modeldefinition['encrypt']:
                """ encrypt init """
                # Create the KEK used for encryption.
                # KeyWrapper is the provided sample implementation, but the user may use their own object as long as it implements the interface above.
                kek = KeyWrapper(self._key_identifier, self._secret_key) #  Key identifier

                # Create the key resolver used for decryption.
                # KeyResolver is the provided sample implementation, but the user may use whatever implementation they choose so long as the function set on the service object behaves appropriately.
                key_resolver = KeyResolver()
                key_resolver.put_key(kek)

                # Set the require Encryption, KEK and key resolver on the service object.
                modeldefinition['tableservice'].key_encryption_key = kek
                modeldefinition['tableservice'].key_resolver_funcion = key_resolver.resolve_key
                modeldefinition['tableservice'].encryption_resolver_function = storagemodel.__class__.__encryptionresolver__

            self.__createtable__(modeldefinition)
                
            self._modeldefinitions.append(modeldefinition)

            log.info('model {} registered successfully. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))
        else:
            log.info('model {} already registered. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))

    @get_modeldefinition(REQUIRED)
    def unregister_model(self, storagemodel:object, modeldefinition = None, delete_table=False):
        """ clear up an Tableservice for an StorageTableModel in your  Azure Storage Account
            Will delete the Table if delete_table Flag is True!
        
            required Parameter is:
            - storagemodel: StorageTableModel(Object)

            Optional Parameter is:
            - delete_table: bool

        """
        
        """ remove from modeldefinitions """
        for i in range(len(self._modeldefinitions)):
            if self._modeldefinitions[i]['modelname'] == modeldefinition['modelname']:
                del self._modeldefinitions[i]
                break
        
        """ delete table from storage if delete_table == True """        
        if delete_table:
            self.__deletetable__(modeldefinition)
        pass

    @get_modeldefinition(REQUIRED)
    def exists(self, storagemodel, modeldefinition) -> bool:
        exists = False
        if storagemodel._exists is None:
            try:
                entity = modeldefinition['tableservice'].get_entity(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey)
                storagemodel._exists = True
                exists = True
            
            except AzureMissingResourceHttpError:
                storagemodel._exists = False
        else:
            exists = storagemodel._exists
                        
        return exists       

    @get_modeldefinition(REQUIRED)
    def get(self, storagemodel, modeldefinition) -> StorageTableModel:
        """ load entity data from storage to vars in self """
        try:
            entity = modeldefinition['tableservice'].get_entity(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey)
            storagemodel._exists = True
        
            """ sync with entity values """
            for key, default in vars(storagemodel).items():
                if not key.startswith('_') and key not in ['','PartitionKey','RowKey']:
                    value = getattr(entity, key, None)
                    if not value is None:
                        setattr(storagemodel, key, value)
             
        except AzureMissingResourceHttpError as e:
            log.debug('can not get table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e))
            storagemodel._exists = False

        except Exception as e:
            log.debug('can not get table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e))
            storagemodel._exists = False

        finally:
            return storagemodel

    @get_modeldefinition(REQUIRED)
    def insert(self, storagemodel, modeldefinition) -> StorageTableModel:
        """ insert model into storage """
        try:            
            modeldefinition['tableservice'].insert_or_replace_entity(modeldefinition['tablename'], storagemodel.entity())
            storagemodel._exists = True

        except AzureMissingResourceHttpError as e:
            storagemodel._exists = False
            log.debug('can not insert or replace table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e))
        except Exception as e:
            storagemodel._exists = False
            log.debug('can not insert or replace table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e))


        finally:
            return storagemodel

    @get_modeldefinition(REQUIRED)
    def merge(self, storagemodel, modeldefinition) -> StorageTableModel:
        """ try to merge entry """
        try:
            entity = modeldefinition['tableservice'].get_entity(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey)
        
            """ merge with entity values """
            for key, default in vars(storagemodel.__class__).items():
                if not key.startswith('_') and key not in ['','PartitionKey','RowKey']:
                    newvalue = getattr(storagemodel, key, None)
                    if (newvalue is None) or (newvalue == default):
                        oldvalue = getattr(entity, key, default)
                        setattr(storagemodel, key, oldvalue)
            
            modeldefinition['tableservice'].insert_or_replace_entity(modeldefinition['tablename'], storagemodel.entity())
            storagemodel._exists = True

        except AzureMissingResourceHttpError as e:
            log.debug('can not merge table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e))

        except Exception as e:
            log.debug('can not merge table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e))

        finally:
            return storagemodel

    @get_modeldefinition(REQUIRED)
    def delete(self,storagemodel, modeldefinition):
        """ delete existing Entity """
        try:
            modeldefinition['tableservice'].delete_entity(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey)
            storagemodel._exists = False

        except AzureMissingResourceHttpError as e:
            log.debug('can not delete table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e))

        finally:
            return storagemodel

    @get_modeldefinition(REQUIRED)
    def query(self, storagequery, modeldefinition) -> StorageTableQuery:
        try:
            if not storagequery._select is None:
                storagequery.extend(modeldefinition['tableservice'].query_entities(modeldefinition['tablename'],filter=storagequery._queryfilter, select=storagequery._select))
            else:
                storagequery.extend(modeldefinition['tableservice'].query_entities(modeldefinition['tablename'],filter=storagequery._queryfilter))

        except AzureMissingResourceHttpError as e:
            log.debug('can not query table {} with filters {} because {!s}'.format(modeldefinition['tablename'], storagequery._queryfilter, e))            

        return storagequery


    def table_isempty(self, tablename, PartitionKey='', RowKey = '') -> bool:
        if  (not self._tableservice is None):

            filter = "PartitionKey eq '{}'".format(PartitionKey) if PartitionKey != '' else ''
            if filter == '':
                filter = "RowKey eq '{}'".format(RowKey) if RowKey != '' else ''
            else:
                filter = filter + ("and RowKey eq '{}'".format(RowKey) if RowKey != '' else '')
            try:
                entities = list(modeldefinition['tableservice'].query_entities(tablename, filter = filter, select='PartitionKey', num_results=1))
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

