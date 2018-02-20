
""" config for Storage Emulator """
testconfig = {
    'AZURE_STORAGE_NAME': '',
    'AZURE_STORAGE_KEY': '',
    'AZURE_REQUIRE_ENCRYPTION': False,
    'AZURE_STORAGE_IS_EMULATED': True,
    'AZURE_KEY_IDENTIFIER': 'azurestoragewrap_test',
    'AZURE_SECRET_KEY': 'supa-dupa-secret-special-key2901'
}

""" logging while testing """
import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('azurestoragewrap')


""" Import application azurestoragewrap.table """        
from azurestoragewrap.table import StorageTableModel, StorageTableContext, StorageTableQuery

""" define table test models """
class TableOne(StorageTableModel):
    Id = 0
    Id2 = ''

    def __setPartitionKey__(self):
        self.PartitionKey = self.Id
        return super().__setPartitionKey__()

    def __setRowKey__(self):
        self.RowKey = self.Id2
        return super().__setRowKey__()

class TableTwo(StorageTableModel):
    _encrypt = True

    Id = ''
    Id2 = ''
    Secret = ''
    NonSecret = ''

    def __setPartitionKey__(self):
        self.PartitionKey = self.Id
        return super().__setPartitionKey__()

    def __setRowKey__(self):
        self.RowKey = self.Id2
        return super().__setRowKey__()

    @staticmethod
    def __encryptionresolver__(pk, rk, property_name):
        """ define properties to encrypt 
            overwrite if inherit this class
        """
        if property_name in ['Secret']:
            return True
        else:
            return False

class TableThree(StorageTableModel):
    Id = 0
    Id2 = ''
    OneToN = StorageTableQuery(TableTwo(), pkcondition='eq', pkforeignkey='Id2')

    def __setPartitionKey__(self):
        self.PartitionKey = self.Id
        return super().__setPartitionKey__()

    def __setRowKey__(self):
        self.RowKey = self.Id2
        return super().__setRowKey__()


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





