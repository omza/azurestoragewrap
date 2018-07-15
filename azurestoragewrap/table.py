""" imports & globals """
from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage import CloudStorageAccount
from azure.storage.table import TableService, Entity


import datetime
from functools import wraps

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
class PartitionKey(object):
    def __init__(self, default):
        self._default = default
        self._type = type(default)

class RowKey(object):
    def __init__(self, default, transformation=None):
        self._default = default
        self._type = type(default)

class EncryptKey(object):
    def __init__(self, default):
        self._default = default
        self._type = type(default)

class StorageTableModel(object):
    _tablename = ''
    _encrypt = []
    _dateformat = ''
    _datetimeformat = ''
    _exists = None

    _PartitionKey = ''
    _RowKey = ''

    def __init__(self, **kwargs):                  
        """ constructor """
        
        self._tablename = self.__class__._tablename
        if self._tablename == '':
            self._tablename = self.__class__.__name__
        self._dateformat = self.__class__._dateformat
        self._datetimeformat = self.__class__._datetimeformat
        self._exists = None
        self._encrypt = []

        IncludeRelationship = []
               
        """ parse **kwargs into instance var """
        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '':

                to_type = type(default)
                
                if to_type is PartitionKey:
                    self._PartitionKey = key
                    to_type = default._type
                    default = default._default

                elif to_type is RowKey:
                    self._RowKey = key
                    to_type = default._type
                    default = default._default

                elif to_type is EncryptKey:
                    self._encrypt.append(key)
                    to_type = default._type
                    default = default._default

                value = kwargs.get(key, default)
                
                if to_type is StorageTableQuery:
                    setattr(self, key, value)
                    IncludeRelationship.append({'key': key, 
                                                'query': value})

                elif to_type is datetime.datetime:
                    setattr(self, key, safe_cast(value, to_type, default, self._datetimeformat))

                elif to_type is datetime.date:
                    setattr(self, key, safe_cast(value, to_type, default, self._dateformat))

                else:
                    setattr(self, key, safe_cast(value, to_type, default))

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
                                                 rkwhere))
                                                 #, query._select))


    # Define the encryption resolver_function.
    @staticmethod
    def __encryptionresolver__(pk, rk, property_name):
        """ define properties to encrypt 
            overwrite if inherit this class
        """
        pass

    def entity(self) -> dict:        
        """ parse self into dictionary """    
        image = {}
        image['PartitionKey'] = self.getPartitionKey()
        image['RowKey'] = self.getRowKey()
        for key, value in vars(self).items():
            if not key.startswith('_') and key not in ['','PartitionKey','RowKey']:
                if type(value) in [str, int, bool, datetime.date, datetime.datetime]:
                    image[key] = value                    
        return image

    def getPartitionKey(self) -> str:
        return str(getattr(self, self._PartitionKey))

    def getRowKey(self) -> str:
        return str(getattr(self, self._RowKey))
 
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

    def __init__(self, storagemodel=None, pkcondition='', pkforeignkey = '', rkcondition = '', rkforeignkey=''):

        """ set storagemodel """
        self._storagemodel = storagemodel
        
        """ set select """
        self._select = None

        """ 
        to be done: parse select statement into self._select 
            if (isinstance(select,list)) and (select != []) and (not self._storagemodel is None):
                self._select = ''
                for key in select:
                    if key in vars(self._storagemodel):
                        if (self._select != ''):
                            self._select += ', '
                        self._select += key
            else:
                self._select = None
        """
           
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

    # decorators
    def getmodeldefinition(self, storageobject, required=False):

        """ find modeldefinition for StorageTableModel or StorageTableQuery """
        if isinstance(storageobject, StorageTableModel):
            definitionlist = [definition for definition in self._modeldefinitions if definition['modelname'] == storageobject.__class__.__name__]
     
        elif isinstance(storageobject, StorageTableQuery):
            """ StorageTableQuery """
            storagemodel = storageobject._storagemodel
            definitionlist = [definition for definition in self._modeldefinitions if definition['modelname'] == storagemodel.__class__.__name__]
        else:
            raise Exception("Argument is not an StorageTableModel nor an StorageTableQuery")
                                                                            
        # is there only one modeldefinition ?
        # hopefully!
        modeldefinition = None

        if len(definitionlist) == 1:
            modeldefinition = definitionlist[0]

        elif len(definitionlist) > 1:
            raise ModelRegisteredMoreThanOnceError(storageobject)

        # is there a modeldefinition if required ?
        if required and modeldefinition is None:
            raise ModelNotRegisteredError(storageobject)

        return modeldefinition


    # constructor 
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
            
            except Exception as e:
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
            
            except Exception as e:
                msg = 'failed to create {} with error {}'.format(tablename, e)
                raise AzureStorageWrapException(msg=msg)
        else:
            return False
        pass

    def register_model(self, storagemodel:object):
        """ set up an Tableservice for an StorageTableModel in your  Azure Storage Account
            Will create the Table if not exist!
        
            required Parameter is:
            - storagemodel: StorageTableModel(Object)

        """

        modeldefinition = self.getmodeldefinition(storagemodel, False)

        if modeldefinition is None:

            """ test if queuename already exists """
            if [model for model in self._modeldefinitions if model['tablename'] == storagemodel._tablename]:
                raise NameConventionError(storagemodel._tablename)

            """ test if queuename fits to azure naming rules """
            if not test_azurestorage_nameconventions(storagemodel._tablename, 'StorageTableModel'):
                raise NameConventionError(storagemodel._tablename)            
                
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

                # Create the EncryptionResolver Function to determine Properties to en/decrypt
                encryptionresolver = self.__encryptionresolver__(modeldefinition['encrypt'])


                # Set the require Encryption, KEK and key resolver on the service object.
                modeldefinition['tableservice'].key_encryption_key = kek
                modeldefinition['tableservice'].key_resolver_funcion = key_resolver.resolve_key
                modeldefinition['tableservice'].encryption_resolver_function = encryptionresolver
                pass

            self.__createtable__(modeldefinition)
                
            self._modeldefinitions.append(modeldefinition)

            log.info('model {} registered successfully. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))
        else:
            log.info('model {} already registered. Models are {!s}.'.format(modeldefinition['modelname'], [model['modelname'] for model in self._modeldefinitions]))

    def unregister_model(self, storagemodel:object, delete_table=False):
        """ clear up an Tableservice for an StorageTableModel in your  Azure Storage Account
            Will delete the Table if delete_table Flag is True!
        
            required Parameter is:
            - storagemodel: StorageTableModel(Object)

            Optional Parameter is:
            - delete_table: bool

        """
        
        # get modeldefinition
        modeldefinition = self.getmodeldefinition(storagemodel, True)

        # remove from modeldefinitions
        for i in range(len(self._modeldefinitions)):
            if self._modeldefinitions[i]['modelname'] == modeldefinition['modelname']:
                del self._modeldefinitions[i]
                break
        
        # delete table from storage if delete_table == True        
        if delete_table:
            self.__deletetable__(modeldefinition)
        pass

    # methods
    def exists(self, storagemodel) -> bool:
        
        modeldefinition = self.getmodeldefinition(storagemodel, True)
        exists = False
        if storagemodel._exists is None:
            try:
                pk = storagemodel.getPartitionKey()
                rk = storagemodel.getRowKey()

                entity = modeldefinition['tableservice'].get_entity(modeldefinition['tablename'], pk, rk)
                storagemodel._exists = True
                exists = True
            
            except AzureMissingResourceHttpError:
                storagemodel._exists = False

            except Exception as e:
                msg = 'failed to test {} with error {}'.format(modeldefinition['tablename'], e)
                raise AzureStorageWrapException(msg=msg)
        else:
            exists = storagemodel._exists
                        
        return exists       

    def get(self, storagemodel) -> StorageTableModel:
        """ load entity data from storage to vars in self """

        modeldefinition = self.getmodeldefinition(storagemodel, True)

        try:

            pk = storagemodel.getPartitionKey()
            rk = storagemodel.getRowKey()

            entity = modeldefinition['tableservice'].get_entity(modeldefinition['tablename'], pk, rk)
            storagemodel._exists = True
        
            """ sync with entity values """
            for key, default in vars(storagemodel).items():
                if not key.startswith('_') and key not in ['','PartitionKey','RowKey']:
                    value = getattr(entity, key, None)
                    if not value is None:
                        setattr(storagemodel, key, value)
             
        except AzureMissingResourceHttpError as e:
            log.debug('can not get table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], pk, rk, e))
            storagemodel._exists = False

        except Exception as e:
            msg = 'can not get table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], pk, rk, e)
            raise AzureStorageWrapException(msg=msg)

        finally:
            return storagemodel

    def insert(self, storagemodel) -> StorageTableModel:
        """ insert model into storage """

        modeldefinition = self.getmodeldefinition(storagemodel, True)

        try:
            modeldefinition['tableservice'].insert_or_replace_entity(modeldefinition['tablename'], storagemodel.entity())
            storagemodel._exists = True

        except AzureMissingResourceHttpError as e:
            storagemodel._exists = False
            log.debug('can not insert or replace table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.getPartitionKey(), storagemodel.getRowKey(), e))

        except Exception as e:
            storagemodel._exists = False
            msg = 'can not insert or replace table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], storagemodel.PartitionKey, storagemodel.RowKey, e)
            raise AzureStorageWrapException(msg=msg)

        finally:
            return storagemodel

    def merge(self, storagemodel) -> StorageTableModel:
        """ try to merge entry """
        modeldefinition = self.getmodeldefinition(storagemodel, True)
       
        try:
            pk = storagemodel.getPartitionKey()
            rk = storagemodel.getRowKey()
            entity = modeldefinition['tableservice'].get_entity(modeldefinition['tablename'], pk, rk)
        
            """ merge with entity values """
            for key, default in vars(storagemodel.__class__).items():
                if not key.startswith('_') and key not in ['']:

                    if isinstance(default, PartitionKey) or isinstance(default, RowKey) or  isinstance(default, EncryptKey):
                        default = default._default

                    newvalue = getattr(storagemodel, key, None)
                    if (newvalue is None) or (newvalue == default):
                        oldvalue = getattr(entity, key, default)
                        setattr(storagemodel, key, oldvalue)
            
            modeldefinition['tableservice'].insert_or_replace_entity(modeldefinition['tablename'], storagemodel.entity())
            storagemodel._exists = True

        except AzureMissingResourceHttpError as e:
            log.debug('can not merge table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], pk, rk, e))

        except Exception as e:
            log.debug('can not merge table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], pk, rk, e))

        finally:
            return storagemodel

    def delete(self,storagemodel):
        """ delete existing Entity """
            
        modeldefinition = self.getmodeldefinition(storagemodel, True)

        pk = storagemodel.getPartitionKey()
        rk = storagemodel.getRowKey()

        try:
            modeldefinition['tableservice'].delete_entity(modeldefinition['tablename'], pk, rk)
            storagemodel._exists = False

        except AzureMissingResourceHttpError as e:
            log.debug('can not delete table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(modeldefinition['tablename'], pk, rk, e))

        finally:
            return storagemodel

    def query(self, storagequery) -> StorageTableQuery:

        modeldefinition = self.getmodeldefinition(storagequery, True)
        
        try:
            
            if (not storagequery._select is None) and (storagequery._select != ''):
                storagequery.extend(modeldefinition['tableservice'].query_entities(modeldefinition['tablename'],filter=storagequery._queryfilter, select=storagequery._select))
            else:
                storagequery.extend(modeldefinition['tableservice'].query_entities(modeldefinition['tablename'],filter=storagequery._queryfilter))

        except AzureMissingResourceHttpError as e:
            storagequery = []
            log.debug('can not query table {} with filters {} because {!s}'.format(modeldefinition['tablename'], storagequery._queryfilter, e))

        except Exception as e:
            msg = 'can not query table {} with filters {} because {!s}'.format(modeldefinition['tablename'], storagequery._queryfilter, e)
            raise AzureStorageWrapException(msg=msg)            

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

            except Exception as e:
                msg = '{!s}'.format(e)
                raise AzureStorageWrapException(msg=msg)
        else:
            return True
        pass

    def __encryptionresolver__(self, encryptproperties):
        def encryptionresolver(pk, rk, property_name):
            if property_name in encryptproperties:
                return True
            else:
                return False
        return encryptionresolver

