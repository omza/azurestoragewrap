
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


""" Import application azurestoragewrap.table """        
from azurestoragewrap.table import StorageTableModel, StorageTableContext, StorageTableQuery, PartitionKey, RowKey, EncryptKey


# Exeptions
from azurestoragewrap.exception import NameConventionError, ModelRegisteredMoreThanOnceError, ModelNotRegisteredError

# pytest
import time, datetime
import pytest


""" define table test models """
class TableOne(StorageTableModel):
    _dateformat = '%d.%m.%Y'
    _datetimeformat = '%d.%m.%Y %H:%M:%S'

    Id = PartitionKey(0) #You have to define one Property as PartitionKey (Part of Azure Storage Table Primary Key) with a default Value
    Id2 = RowKey('')     #You have to define one Property as RowKey (Part of Azure Storage Table Primary Key) with a default Value
    beginn = datetime.datetime.strptime('01.01.1900 00:00:00', _datetimeformat)
    ende  = datetime.datetime.strptime('01.01.1900 00:00:00', _datetimeformat)


class TableTwo(StorageTableModel):
    Id = PartitionKey('')
    Id2 = RowKey('')
    Secret = EncryptKey('') # a Property you like to en-/decrypt clientside has to define as "EncryptKey" with an default Value
    NonSecret = ''
    Secret2 = EncryptKey('second encrypt') # of cause you can mix multiple encrypted and non encrypted Properties in a Table Model


class TableThree(StorageTableModel):
    Id = PartitionKey(0)
    Id2 = RowKey('')
    OneToN = StorageTableQuery(TableTwo(), pkcondition='eq', pkforeignkey='Id2')


class Table4(StorageTableModel):
    Id = PartitionKey(0)
    Id2 = RowKey('')
    TableName = True

class TableNameConventionError(StorageTableModel):
    _tablename = '!"§$%&/()=?asdkjkllllllllllllllllllllllllllllllllllllllllllllllllllllllllalsdalsnclyxnvxcvjndfnldnböfgnbköfgböfnbälfkgbkfgmblfk gbl flbknowerpweufndkövnvndlfvndöjfnvoeruvnköjvxkv'
    Id = PartitionKey(0)
    Id2 = RowKey('')


""" Testcases positiv"""
class TestStorageTablePositive(object):


    def test_init_StorageTableContext(self):  
        db = StorageTableContext(**testconfig)
        assert isinstance(db, StorageTableContext)

    def test_register_model(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableOne())
        assert 'TableOne' in [model['modelname'] for model in db._modeldefinitions]

    def test_exists_entry(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableOne())
        testentry = TableOne(Id=1, Id2 ='test_exists_entry')
        testentry = db.get(testentry)
        assert not testentry._exists 
        assert db.exists(testentry) == False

    def test_insert_entry(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableOne())
        testentity = TableOne(Id=1, Id2 ='test_insert_entry')
        testentity = db.insert(testentity)
        assert testentity._exists 
        assert db.exists(testentity)
        db.delete(testentity)

    def test_merge_entity(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableTwo())

        writeentity = TableTwo(Id=1, Id2 ='test_partly_encryption', Secret='Secret', NonSecret='')
        writeentity = db.insert(writeentity)
        assert db.exists(writeentity)

        mergeentity = TableTwo(Id=1, Id2 ='test_partly_encryption', Secret='', NonSecret='NonSecret')
        mergeentity = db.merge(mergeentity)
        assert mergeentity.Secret == 'Secret' and mergeentity.NonSecret == 'NonSecret'
       
        db.delete(mergeentity)

    def test_partly_encryption(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableTwo())

        writeentity = TableTwo(Id=1, Id2 ='test_partly_encryption', Secret='Secret', NonSecret='NonSecret')
        writeentity = db.insert(writeentity)
        assert db.exists(writeentity)

        readentity = db.get(TableTwo(Id=1, Id2 ='test_partly_encryption'))
        assert readentity.Secret == 'Secret'  and readentity.NonSecret == 'NonSecret'
       
        db.delete(readentity)

    def test_relationship(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableTwo())
        db.register_model(TableThree())

        for x in range(1,11):
            db.insert(TableTwo(Id='First', Id2 = x, Secret='Secret', NonSecret='NonSecret'))
        for x in range(1,11):
            db.insert(TableTwo(Id='Second', Id2 = x, Secret='Secret', NonSecret='NonSecret'))

        entity = TableThree(Id=1, Id2='Second')
        db.insert(entity)
        entity.OneToN = db.query(entity.OneToN)
        assert len(entity.OneToN) == 10

    def test_newtablemodel(self):
        db = StorageTableContext(**testconfig)
        db.register_model(Table4())

        model = Table4(Id=1, Id2 ='test_newtablemodel')
        db.insert(model)
        assert model.TableName == True

 # Testcases negative
class TestStorageTableNegative(object):

    def test_error_naming_convention(self):
        db = StorageTableContext(**testconfig)
        with pytest.raises(NameConventionError):
            db.register_model(TableNameConventionError())

    def test_register_model_first(self):
        db = StorageTableContext(**testconfig)
        with pytest.raises(ModelNotRegisteredError):
            testentry = db.get(TableOne(Id=1, Id2 ='test_exists_entry'))


""" Housekeeping """
class TestStorageTableHousekeeping(object):

    def test_delete_tables(self):
        db = StorageTableContext(**testconfig)
        modeldef = TableOne()
        db.register_model(modeldef)
        db.unregister_model(modeldef)
        assert not 'TableOne' in [model['modelname'] for model in db._modeldefinitions]

        db.register_model(modeldef)
        db.unregister_model(modeldef, delete_table=True)
        assert not 'TableOne' in [model['modelname'] for model in db._modeldefinitions]

        modeldef = TableTwo()
        db.register_model(modeldef)
        db.unregister_model(modeldef, None, True)
        assert not 'TableTwo' in [model['modelname'] for model in db._modeldefinitions]

        modeldef = TableThree()
        db.register_model(modeldef)
        db.unregister_model(modeldef, None, True)
        assert not 'TableThree' in [model['modelname'] for model in db._modeldefinitions]


        modeldef = Table4()
        db.register_model(modeldef)
        db.unregister_model(modeldef, None, True)
        assert not 'Table4' in [model['modelname'] for model in db._modeldefinitions]


