
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
from azurestoragewrap.table import StorageTableModel, StorageTableContext

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

    Id = 0
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

    def test_write_entry(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableOne())
        testentity = TableOne(Id=1, Id2 ='test_write_entry')
        testentity = db.insert(testentity)
        assert testentity._exists 
        assert db.exists(testentity)
        db.delete(testentity)

    def test_partly_encryption(self):
        db = StorageTableContext(**testconfig)
        db.register_model(TableTwo())

        writeentity = TableTwo(Id=1, Id2 ='test_partly_encryption', Secret='Secret', NonSecret='NonSecret')
        writeentity = db.insert(writeentity)
        assert db.exists(writeentity)

        readentity = db.get(TableTwo(Id=1, Id2 ='test_partly_encryption'))
        assert readentity.Secret == 'Secret'  and readentity.NonSecret == 'NonSecret'
       
        db.delete(readentity)





