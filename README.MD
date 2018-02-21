# azurestoragewrap

> A Python Wrapper for modeling client side encrypted azure storage tables, queues and blobs

Microsoft Azure Storage is a Microsoft-managed cloud service that provides storage that is highly available, secure, durable, scalable, and redundant. Azure Storage consists of Blob storage, Table Storage, and Queue storage. 

Azure Table storage stores large amounts of structured data. The service is a NoSQL datastore which accepts authenticated calls from inside and outside the Azure cloud. Azure tables are ideal for storing structured, non-relational data.

Azure Blob storage is a service for storing large amounts of unstructured object data, such as text or binary data - e.g. to serve images or documents directly to a browser.

Azure Queue storage is a service for storing large numbers of messages - e.g. a backlog of work to process asynchronously.

All Data can be accessed from anywhere in the world via HTTP or HTTPS

## Getting started

Get azurestoragewrap via pip

```
pip install azurestoragewrap
```

## Usage examples

Using azurestoragewrap should be as easy as possible and has to be handle in a few steps:
- config Your settings, incl. Your Azure Storage Credentials in a python dictionary
- model Your Data Objects as a subclass of StorageTableModel, StorageQueueModel or StorageBlobModel
- initiate the StorageContext
- register Your Model to the StorageContext

### Configuration

Configure as followed 

```
config = {
    'AZURE_STORAGE_NAME': '',
    'AZURE_STORAGE_KEY': '',
    'AZURE_REQUIRE_ENCRYPTION': True,
    'AZURE_STORAGE_IS_EMULATED': True,
    'AZURE_KEY_IDENTIFIER': 'azurestoragewrap_test',
    'AZURE_SECRET_KEY': 'supa-dupa-secret-special-key2901'
}
```

### Table

To handle Table Data you have to model your Table structure like this:
```
# Model without encryption
class TableOne(StorageTableModel):
    Id = 0
    Id2 = ''

    def __setPartitionKey__(self):
        self.PartitionKey = self.Id
        return super().__setPartitionKey__()

    def __setRowKey__(self):
        self.RowKey = self.Id2
        return super().__setRowKey__()

#Model with Partly encryption
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
```

Now you can initiate Your StorageTableContext and register your Model:
```
db = StorageTableContext(**testconfig)
db.register_model(TableOne())
```

Now get started:
```
#Insert
entity = TableOne()
entity.Id = 1
entity.Id2 = 'Test'
entity = db.insert(entity)

#Select
entity = db.get(TableOne(Id=1, Id2='Test'))

#Delete
db.delete(entity)

#Replace or Merge (encrypted or not)
db.register_model(TableTwo())

writeentity = TableTwo(Id=1, Id2 ='test_partly_encryption', Secret='Secret', NonSecret='')
writeentity = db.insert(writeentity)

mergeentity = TableTwo(Id=1, Id2 ='test_partly_encryption', Secret='', NonSecret='NonSecret')
mergeentity = db.merge(mergeentity)
```


### Queue

Explain what these tests test and why

```
Give an example
```

### Blob

Explain what these tests test and why

```
Give an example
```

## Meta

* **Oliver Meyer** - *app workshop UG (haftungsbeschränkt)* - [omza on github](https://github.com/omza)

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details