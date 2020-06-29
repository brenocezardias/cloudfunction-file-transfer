# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil import parser as dtparse
from urllib import parse
import os
import base64
import json
import logging

import compress
import transfer

# Map of the URI scheme to their respective classes
TRANSFER_TYPES = transfer.get_transfer_types()
COMPRESSION_TYPES = compress.get_compression_types()

def prevent_infinite_retry(timestamp):
    event_time = dtparse.isoparse(timestamp)
    event_age = (datetime.now() - event_time).total_seconds() * 1000
    
    # Ignore events that are too old (1 hour)
    max_age_ms = 3600000
    if event_age > max_age_ms:
        return True
    else:
        return False

def transfer_file(event, context):
    # parsing the message
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    transfer_info = json.loads(pubsub_message)

    if prevent_infinite_retry(transfer_info.get('event_date', datetime.now().isoformat())):
        logging.error('{} aborted'.format(transfer_info))
        return 'Timeout'

    # parsing the URI of the connection string provided
    source_conn_str = parse.urlparse(transfer_info['source_connection_string'], allow_fragments=False)
    dest_conn_str = parse.urlparse(transfer_info['destination_connection_string'])
    compression = transfer_info['compress_algorithm'] if 'compress_algorithm' in transfer_info else None
    decompression = transfer_info['decompress_algorithm'] if 'decompress_algorithm' in transfer_info else None    

    # deciding which class to instantiate from the scheme of the URI
    source_type = TRANSFER_TYPES.get(source_conn_str.scheme)
    destination_type = TRANSFER_TYPES.get(dest_conn_str.scheme)
    compress_type = COMPRESSION_TYPES.get(compression)
    decompress_type = COMPRESSION_TYPES.get(decompression)

    if(source_type is None):
        raise LookupError('Type %s not supported' % source_conn_str.scheme)

    if(destination_type is None):
        raise LookupError('Type %s not supported' % dest_conn_str.scheme)

    if('compress_algorithm' in transfer_info and compress_type is None):
        raise LookupError('Type %s not supported' % compression)
    
    if('decompress_algorithm' in transfer_info and decompress_type is None):
        raise LookupError('Type %s not supported' % decompression)

    source = source_type(source_conn_str)
    destination = destination_type(dest_conn_str)
    compression = compress_type() if compress_type is not None else None
    decompression = decompress_type() if decompress_type is not None else None
    
    try:
        source.connect()
        destination.connect()

        # list files and transfer them one by one
        for file in source.list_files():
            logging.info('Transferring file %s to destination %s' % (file, dest_conn_str))
            file_name = source.download_file(file)

            try:
                if(decompression is not None):
                    file_name = decompression.decompress_file(file_name)

                if(compression is not None):
                    file_name = compression.compress_file(file_name)

                destination.upload_file(file_name)

                if('remove_file' in transfer_info and transfer_info['remove_file']):
                    source.remove_file(file)
            except Exception as error:
                raise RuntimeError('Error during execution') from error
            finally:
                os.remove('/tmp/' + file_name.split('/')[-1])
    except Exception as error:
        raise RuntimeError('Error during execution') from error
    finally:
        source.disconnect()
        destination.disconnect()

if __name__ == '__main__':
    from datetime import timedelta
    
    event = '''{{
    "source_connection_string": "gs://teste-dataflow/temp2/20180801_SOREPAROS_1301_1.txt.zip",
    "destination_connection_string": "gs://teste-dataflow/temp3/",
    "remove_file": "False",
    "decompress_algorithm": "zip",
    "event_date": "{}"
}}'''.format((datetime.now() - timedelta(minutes=1)).isoformat())
    
    msg = {
        '@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 
        'attributes': None, 
        'data': base64.b64encode(event.encode('utf-8'))
    }
    
    transfer_file(msg, None)