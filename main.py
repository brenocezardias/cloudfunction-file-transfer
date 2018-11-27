# -*- coding: utf-8 -*-

from google.cloud import storage
from ast import literal_eval
from urllib import parse
import pysftp
import abc
import ftplib
import base64
import logging
import os
import re

# Parameters
# project name
PROJECT = os.environ['PROJECT']

# Abstract base class for the file transfers
class FileTransfer(object, metaclass=abc.ABCMeta):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        super().__init__()

    @abc.abstractmethod
    def connect(self, connection_string: str):
        raise NotImplementedError('Abstract method')
    
    @abc.abstractmethod
    def download_file(self, file_path: str):
        raise NotImplementedError('Abstract method')
    
    @abc.abstractmethod
    def upload_file(self, file_path: str):
        raise NotImplementedError('Abstract method')
    
    @abc.abstractmethod
    def list_files(self, file_path: str):
        raise NotImplementedError('Abstract method')
    
    @abc.abstractmethod
    def remove_file(self, file_path: str):
        raise NotImplementedError('Abstract method')

    @abc.abstractmethod
    def disconnect(self):
        raise NotImplementedError('Abstract method')

# Concrete type for Google Cloud Storage Transfers
class GcsFileTransfer(FileTransfer):
    def __init__(self, connection_string):
        self.gcs = None
        self.bucket = None
        self.conn_str = connection_string
    
    def connect(self):
        self.gcs = storage.Client(project=PROJECT)
        self.bucket = self.gcs.get_bucket(self.conn_str.netloc)
        logging.info('Connected to GCS on project: ' + PROJECT)
    
    def download_file(self, file_path):
        # removing the leading / so as to not create a folder with it
        file_name = file_path.split('/')[-1]
        # creating the final file path
        path = self.conn_str.path[1:] + ('' if self.conn_str.path.endswith('/') else '/') + file_name
        blob = self.bucket.blob(path)
        # downloading to local storage
        blob.download_to_filename('/tmp/' + file_name)
        logging.info('Downloaded file %s to bucket %s successfully' % (file_path, self.conn_str.netloc))

        return file_name
    
    def upload_file(self, file_path):
        # creating the final file path
        path = self.conn_str.path[1:] + ('' if self.conn_str.path.endswith('/') else '/') + file_path
        blob = self.bucket.blob(path)
        # uploading from local storage
        blob.upload_from_filename('/tmp/' + file_path)
        logging.info('Uploaded file %s to bucket %s successfully' % (file_path, self.conn_str.netloc))
    
    def remove_file(self, file_path):
        # removing the leading / so as to not create a folder with it
        file_name = file_path.split('/')[-1]
        # creating the final file path
        path = self.conn_str.path[1:] + ('' if self.conn_str.path.endswith('/') else '/') + file_name
        self.bucket.delete_blob(path)
        logging.info('Removed file %s to bucket %s successfully' % (file_path, self.conn_str.netloc))

    def list_files(self):
        # from the blob file list return only the file names
        return [f.name for f in self.bucket.list_blobs(prefix=self.conn_str.path[1:])]
    
    def disconnect(self):
        # gcs client does not require an explicit disconnect
        pass

# Concrete type for FTP transfers
class FtpFileTransfer(FileTransfer):
    def __init__(self, connection_string):
        self.ftp = None
        self.conn_str = connection_string
    
    def connect(self):
        self.ftp = ftplib.FTP(self.conn_str.netloc)
        logging.info('Connected to FTP: ' + self.conn_str.netloc)
        # parsing the query parameters to a dictionary for the login components
        auth_info = dict(parse.parse_qs(self.conn_str.query))
        self.ftp.login(auth_info['username'][0], auth_info['password'][0])
        logging.info('Login successfull')
    
    def disconnect(self):
        self.ftp.quit()
        logging.info('Disconnected from ' + self.conn_str.netloc)
    
    def download_file(self, file_path: str):
        # creating the final file path
        file_name = file_path.split('/')[-1]
        self.ftp.retrbinary('RETR ' + file_path, open('/tmp/' + file_name, 'wb').write)
        logging.info('File %s downloaded successfully' % file_name)

        return file_name
    
    def upload_file(self, file_path):
        file = open('/tmp/' + file_path, 'rb')
        # moving to the desired path
        self.ftp.cwd(self.conn_str.path)
        # uploading file
        self.ftp.storbinary('STOR ' + file_path, file)
        file.close()                                
    
    def remove_file(self, file_path):
        # creating the final file path
        file_name = file_path.split('/')[-1]
        self.ftp.delete(file_path)
        logging.info('File %s removed successfully' % file_name)

    def list_files(self):
        return list(self.ftp.nlst(self.conn_str.path))

# Concrete type for SFTP transfers
class SftpFileTransfer(FileTransfer):
    def __init__(self, connection_string):
        self.sftp = None
        self.conn_str = connection_string
    
    def connect(self):
        # parsing the query parameters to a dictionary for the login components
        auth_info = dict(parse.parse_qs(self.conn_str.query))
        # ignoring known_hosts
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.sftp = pysftp.Connection(self.conn_str.netloc, username=auth_info['username'][0], password=auth_info['password'][0], cnopts=cnopts)
        logging.info('Connected to SFTP: ' + self.conn_str.netloc)
    
    def disconnect(self):
        self.sftp.close()
        logging.info('Disconnected from ' + self.conn_str.netloc)
    
    def download_file(self, file_path: str):
        # creating the final file path
        file_name = file_path.split('/')[-1]
        self.sftp.get(file_path, localpath='/tmp/' + file_name)
        logging.info('File %s downloaded successfully' % file_name)

        return file_name
    
    def upload_file(self, file_path):
        # moving to the desired path
        self.sftp.cd(self.conn_str.path)
        # uploading file
        self.sftp.put('/tmp/' + file_path)               
    
    def remove_file(self, file_path):
        # creating the final file path
        file_name = file_path.split('/')[-1]
        self.sftp.remove(file_path)
        logging.info('File %s removed successfully' % file_name)

    def list_files(self):
        # listdir does not include the folder path, so we manually add it
        return ['{}/{}'.format(self.conn_str.path, f) for f in self.sftp.listdir(self.conn_str.path)]

# Map of the URI scheme to their respective classes
TRANSFER_TYPES = {
    'ftp': FtpFileTransfer,
    'sftp': SftpFileTransfer,
    'gs': GcsFileTransfer
}

def transfer_file(event, context):
    # parsing the message
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    transfer_info = literal_eval(pubsub_message)

    # parsing the URI of the connection string provided
    source_conn_str = parse.urlparse(transfer_info['source_connection_string'], allow_fragments=False)
    dest_conn_str = parse.urlparse(transfer_info['destination_connection_string'])

    # deciding which class to instantiate from the scheme of the URI
    source_type = TRANSFER_TYPES.get(source_conn_str.scheme)
    destination_type = TRANSFER_TYPES.get(dest_conn_str.scheme)

    if(source_type is None):
        raise LookupError('Type %s not supported' % source_conn_str.scheme)

    if(destination_type is None):
        raise LookupError('Type %s not supported' % dest_conn_str.scheme)

    source = source_type(source_conn_str)
    destination = destination_type(dest_conn_str)
    
    source.connect()
    destination.connect()

    # list files and transfer them one by one
    for file in source.list_files():
        file_name = source.download_file(file)
        destination.upload_file(file_name)

        os.remove('/tmp/' + file_name.split('/')[-1])
        if(transfer_info['remove_file']):
            source.remove_file(file_name)
    
    source.disconnect()
    destination.disconnect()

if __name__ == '__main__':
    event = '''{
    "source_connection_string": "gs://teste-dataflow/temp2/",
    "destination_connection_string": "gs://teste-dataflow/temp/",
    "remove_file": False
}'''
    
    msg = {
        '@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 
        'attributes': None, 
        'data': base64.b64encode(event.encode('utf-8'))
    }
    
    transfer_file(msg, None)