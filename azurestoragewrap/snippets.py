""" imports & globals """
import datetime
import re

""" snippets """
def safe_cast(val, to_type, default=None, dformat=''):
    try:
        result = default
        if type(val) == to_type:
            result = val

        elif to_type == datetime.datetime:          
            result = to_type.strptime(val, dformat)

        elif to_type == datetime.date:          
            result = datetime.datetime.strptime(val, dformat).date()
        
        elif to_type is bool:
            result = str(val).lower() in ("yes", "true", "t", "1")
        
        elif to_type is str:
            if (isinstance(val, datetime.datetime) or isinstance(val, datetime.date)):
                result = val.strftime(dformat)
            else:
                result = str(val)
        else:
            result = to_type(val)

        return result
        
    except (ValueError, TypeError):
        return default


def test_azurestorage_nameconventions(storageobjectname, storageobjecttype):

    if storageobjecttype == 'StorageTableModel':
        pattern = re.compile('^[A-Za-z][A-Za-z0-9]{2,62}$')

    elif storageobjecttype in ['StorageQueueModel', 'StorageBlobModel']:
        pattern = re.compile('^[a-z0-9][\-a-z0-9]{2,62}$')

    else:
        pattern = re.compile('')

    if pattern.match(storageobjectname):
        return True
    else:
        return False
    pass


