[![Build Status](https://travis-ci.org/omza/azurestoragewrap.svg?branch=master)](https://travis-ci.org/omza/azurestoragewrap)

# azurestoragewrap

> A Python Wrapper for modeling client side encrypted azure storage tables, queues and blobs

Inspired by the implementation of data modeling in [SQLAlchemy](https://github.com/zzzeek/sqlalchemy) lib - What the great Job! Thank You! -, i wrote this little wrapper around the [Azure Storage SDK for Python](https://github.com/Azure/azure-storage-python) to simplify modeling data structures and easily implement Client Side Encryption for my own needs. 
Would be lucky if this lib helps other peoples too. GitHub Issues, Stars, Forks and Contribution are Welcome! Have fun with azurestoragewrap.

Microsoft Azure Storage is a Microsoft-managed cloud service that provides storage that is highly available, secure, durable, scalable, and redundant. Azure Storage consists of Blob storage, Table Storage, and Queue storage. 
All Data can be accessed from anywhere in the world via HTTP or HTTPS.

## Getting started

Get azurestoragewrap via pip

```
pip install azurestoragewrap
```

## Usage examples

Using azurestoragewrap should be as easy as possible and has to be handle in a few steps:
- config your settings, incl. your Azure Storage Credentials in a python dictionary
- initiate the StorageContext
- model your Data Objects as a subclass of StorageTableModel, StorageQueueModel or StorageBlobModel
- register your Model to the StorageContext

### Configuration

With following Settings you can setup azurestoragewrap Context Objects. First of all you have to configure your [Azure Storage Credentials]() or while testing try the local Emulator which has to be installed and up and running of cause:  

```
AZURE_STORAGE_NAME = ''
AZURE_STORAGE_KEY = ''
```
or
```
AZURE_STORAGE_IS_EMULATED = True   #True or False,
```
if you want to use the client side encryption your welcome to set up a key identifier and a SECRET Key for encryption/decryption
```
AZURE_KEY_IDENTIFIER = 'azurestoragewrap_test',
AZURE_SECRET_KEY = 'supa-dupa-secret-special-key2901' # Has to be a valid AES length (8,16,32 characters)
```

### Table

Azure Table storage stores large amounts of structured data. The service is a NoSQL datastore which accepts authenticated calls from inside and outside the Azure cloud. Azure tables are ideal for storing structured, non-relational data. To handle Table Data you have to model your Table structure like this:

```
# Model without encryption
class TableOne(StorageTableModel):
    Id = PartitionKey(0) #You have to define one Property as PartitionKey (Part of Azure Storage Table Primary Key) with a default Value
    Id2 = RowKey('') #You have to define one Property as RowKey (Part of Azure Storage Table Primary Key) with a default Value

#Model with Partly encryption
class TableTwo(StorageTableModel):
    Id = PartitionKey('')
    Id2 = RowKey('')
    Secret = EncryptKey('') # a Property you like to en-/decrypt clientside has to define as "EncryptKey" with an default Value
    NonSecret = ''
    Secret2 = EncryptKey('second encrypt') # of cause you can mix multiple encrypted and non encrypted Properties in a Table Model
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

Azure Queue storage is a service for storing large numbers of messages - e.g. a backlog of work to process asynchronously.


```
Give an example
```

### Blob

Azure Blob storage is a service for storing large amounts of unstructured object data, such as text or binary data - e.g. to serve images or documents directly to a browser.


```
Give an example
```

## Meta

* **Oliver Meyer** - *app workshop UG (haftungsbeschränkt)* - [omza on github](https://github.com/omza)

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details