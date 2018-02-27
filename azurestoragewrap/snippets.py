""" imports & globals """
import datetime
import re

""" snippets """
def safe_cast(val, to_type, default=None, dformat=''):
    try:
        result = default
        if to_type in [datetime.datetime, datetime.date]:
            if type(val) == to_type:
                val = val.strftime(dformat)
           
            result = to_type.strptime(val, dformat)
        
        elif to_type is bool:
            result = str(val).lower() in ("yes", "true", "t", "1")
        
        elif to_type is str:
            if (isinstance(val, datetime.datetime) or isinstance(val, datetime.date)):
                result = str(val).strftime(dformat)
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

    elif storageobjecttype == 'StorageQueueModel':
        pattern = re.compile('^[a-z0-9][\-a-z0-9]{2,62}$')

    else:
        pattern = re.compile('')

    if pattern.match(storageobjectname):
        return True
    else:
        return False
    pass


