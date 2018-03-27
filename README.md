[![Build Status](https://travis-ci.org/omza/azurestoragewrap.svg?branch=master)](https://travis-ci.org/omza/azurestoragewrap)

# azurestoragewrap
> A Python Wrapper for modeling client side encrypted azure storage tables, queues and blobs

Inspired by the implementation of data modeling in [SQLAlchemy](https://github.com/zzzeek/sqlalchemy) lib - What the great Job! Thank You! -, i wrote this little wrapper around the [Azure Storage SDK for Python](https://github.com/Azure/azure-storage-python) to simplify modeling data structures and easily implement Client Side Encryption for my own needs. 
Would be lucky if this lib helps other peoples too. GitHub Issues, Stars, Forks and Contribution are Welcome! Have fun with azurestoragewrap.

Microsoft Azure Storage is a Microsoft-managed cloud service that provides storage that is highly available, secure, durable, scalable, and redundant. Azure Storage consists of Blob storage, Table Storage, and Queue storage. 
All Data can be accessed from anywhere in the world via HTTP or HTTPS.

## Getting started
azurestoragewrap is available on [PyPi](https://pypi.org/project/azurestoragewrap/)! Get azurestoragewrap via pip

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
With following Settings you can setup azurestoragewrap Context Objects. First of all you have to configure your [Azure Storage Credentials](https://docs.microsoft.com/en-us/azure/storage/common/storage-create-storage-account) or while testing try the local Emulator which has to be installed and up and running of cause:  

```
AZURE_STORAGE_NAME = ''
AZURE_STORAGE_KEY = ''
```
or
```
AZURE_STORAGE_IS_EMULATED = True   #True or False,
```
if you want to use the client side encryption you are welcome to set up a key identifier and a SECRET Key for en- and decryption:
```
AZURE_KEY_IDENTIFIER = 'azurestoragewrap_test',
AZURE_SECRET_KEY = 'supa-dupa-secret-special-key2901' # Has to be a valid AES length (8,16,32 characters)
```

### Table
Azure Table storage stores large amounts of structured data. The service is a NoSQL datastore which accepts authenticated calls from inside and outside the Azure cloud. Azure tables are ideal for storing structured, non-relational data. 

With the parameter above you can create a new StorageTableContext Instance to initiate an Azure Connection:
```
db = StorageTableContext(AZURE_STORAGE_NAME, 
							AZURE_STORAGE_KEY,
							AZURE_STORAGE_IS_EMULATED
							AZURE_KEY_IDENTIFIER',
							AZURE_SECRET_KEY
							)
```
To handle Table Data you have to model your Table structure like this:
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

Now you can register your Models defined above:
```
db.register_model(TableOne())
db.register_model(TableTwo())
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

### Table Queries & Relationships (1-n)
If you like to query a Storage Table or define a Relationship within a StorageTableModel feel free to use the StorageTableQuery Object, wich is a subclass of the pyton list object:
```
class TableTwo(StorageTableModel):
	_tablename = 'tabletwo'
    Id = PartitionKey('')
    Id2 = RowKey('')

db = StorageTableContext(**config)
db.register_model(TableTwo())

# define a adhoc Query
query = StorageTableQuery(TableTwo(), pkcondition='eq', pkforeignkey='PartitionKey', pkcondition='eq', pkforeignkey='RowKey') 
entities = db.query(query)
```
The Query defined above gives you a List of all entities from Azure Storage Table named 'tabletwo' where the PartitionKey is equal (eq) to 'PartitionKey' AND where the RowKey is equal to 'RowKey'

```
# Relationship (1-n) within a Model
class TableTwo(StorageTableModel):
	_tablename = 'tabletwo'
    Id = PartitionKey('')
    Id2 = RowKey('')

class TableThree(StorageTableModel):
    Id = PartitionKey(0)
    TableThreeId = RowKey('')
    OneToN = StorageTableQuery(TableTwo(), pkcondition='eq', pkforeignkey='TableThreeId')

db = StorageTableContext(**config)
db.register_model(TableTwo())
db.register_model(TableThree())

entity = TableThree(Id=1, TableThreeId='Second')
entity.OneToN = db.query(entity.OneToN)
```
In design time the property 'OneToN' of Model 'TableThree' is defined as an 1-n relationship to Model 'TableTwo' joining the PartitionKey of TableTwo with TableThree.TableThreeId as the foreign Key.
When creating a Instance of TableThree (here 'entity') the StorageTableQuery is initiated as well with the given value for 'TableThreeId'  
 
### Queue

Azure Queue storage is a service for storing large numbers of messages - e.g. a backlog of work to process asynchronously. Use Azure Queue Storage to build flexible applications and separate functions for better durability across large workloads. When you design applications for scale, application components can be decoupled, so that they can scale independently. Queue storage gives you asynchronous message queuing for communication between application components, whether they are running in the cloud, on the desktop, on premises or on mobile devices.

To start working with Queue Messages you have to model the properties by subclassing StorageQueueModel. In difference to StorageTables entry, a queue message can only be encrypted client-side completely. In contrast to a storage table entry, a queue message can only be encrypted completely. Therefore, the encryption will be configured for the entire StorageQueueModel by set '_encrypt=True' and not on the property level like in StorageTableModel.
```
# Model without encryption
class QueueOne(StorageQueueModel):
    epgid = 0
    statuscode = 1
	statusmessage = 'New'


#Model with encryption
class QueueTwo(StorageQueueModel):
    _encrypt = True
    _queuename = 'encryptedtest'
    user = ''
    password = ''
    server = ''
    protocol = ''
```
After modeling your StorageQueueModels you have create a StorageQueueContext instance with the Parameter mentioned above and register the models in this instance. 
```
queue = StorageQueueContext(AZURE_STORAGE_NAME, 
							AZURE_STORAGE_KEY,
							AZURE_STORAGE_IS_EMULATED
							AZURE_KEY_IDENTIFIER',
							AZURE_SECRET_KEY
							)

# Register StorageQueueModels like this:
queue.register_model(QueueTwo())
queue.register_model(QueueOne())

```
Now you are able to peek, put, get update and delete queue messages like this:
```
# queue.put()
# add a queue message at the end of the queue just put it:
message = QueueOne(epgid = 1, resolution = 'test_put')
queue.put(message)

# queue.peek()
# lookup (peek) the first message in the queue. firstmessage will be an instance of class QueueOne
firstmessage = queue.peek(QueueOne())

# queue.get()
# will get the first message in the queue. firstmessage will be an instance of class QueueOne. Use the get instead of the peek method if you like to further process the message (update or delete)
firstmessage = queue.get(QueueOne())
#or if you like to hide current message (like below 10 seconds) to e.g. other worker sessions
firstmessage = queue.get(QueueOne(), hide=10)

# queue.delete()
# if your worker session processed a queue message completely it makes sense to delete it from the queue like this:
firstmessage = queue.get(QueueOne())
# ... further processing ...
queue.delete(firstmessage)

# queue.update()
# or your worker session will update the queue message e.g. a error raises during processing
firstmessage = queue.get(QueueOne())
try:
	# ... further processing raises en error...
except Exception as e:
	firstmessage.statuscode = -1
	firstmessage.statusmessage = e
	queue.update(firstmessage)
```

### Blob

Azure Blob storage is a service for storing large amounts of unstructured object data, such as text or binary data - e.g. to serve images or documents directly to a browser.

With the parameter above you can create a new StorageTableContext Instance to initiate an Azure Connection:
```
Give an example
```

## Meta

* **Oliver Meyer** - *app workshop UG (haftungsbeschränkt)* - [omza on github](https://github.com/omza)

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details