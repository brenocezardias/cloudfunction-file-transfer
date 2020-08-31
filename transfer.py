# -*- coding: utf-8 -*-

from google.cloud import storage
from urllib import parse
import boto3
import uuid
import ftplib
import pysftp
import abc
import fnmatch
import json
import os
import logging

# Parameters
# project name
PROJECT = os.environ['PROJECT']

# Abstract base class for the file transfers
class FileTransfer(object, metaclass=abc.ABCMeta):
    def __init__(self, connection_string: str, service_account:str = None):
        self.connection_string = connection_string
        self.service_account = service_account
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
    def __init__(self, connection_string, service_account:str = None):
        self.gcs = None
        self.bucket = None
        self.conn_str = connection_string
        self.service_account = service_account
    
    def connect(self):
        if self.service_account:
            self.auth_file = '/tmp/{}'.format(uuid.uuid4())
            client = storage.Client(project=PROJECT)
            with open(self.auth_file, 'wb') as f:
                client.download_blob_to_file(self.service_account, f)
            self.gcs = storage.Client.from_service_account_json(self.auth_file)
        else:
            self.gcs = storage.Client(project=PROJECT)
        self.bucket = self.gcs.get_bucket(self.conn_str.netloc)
        logging.info('Connected to GCS on project: ' + PROJECT)
    
    def download_file(self, file_path):
        # removing the leading / so as to not create a folder with it
        file_name = file_path.split('/')[-1]
        blob = self.bucket.blob(file_path[1:])
        # downloading to local storage
        blob.download_to_filename('/tmp/' + file_name)
        logging.info('Downloaded file %s to bucket %s successfully' % (file_path, self.conn_str.netloc))

        return file_name
    
    def upload_file(self, file_path):
        # creating the final file path
        path = self.conn_str.path[1:] + file_path
        blob = self.bucket.blob(path)
        # uploading from local storage
        blob.upload_from_filename('/tmp/' + file_path)
        logging.info('Uploaded file %s to bucket %s successfully' % (file_path, self.conn_str.netloc))
    
    def remove_file(self, file_path):
        self.bucket.delete_blob(file_path[1:])
        logging.info('Removed file %s to bucket %s successfully' % (file_path, self.conn_str.netloc))

    def list_files(self):
        # from the blob file list return only the file names
        files = ['/' + f.name for f in self.bucket.list_blobs(prefix=self.conn_str.path[1:self.conn_str.path.rfind('/')])]
        return fnmatch.filter(files, '*' + self.conn_str.path[self.conn_str.path.rfind('/'):])
    
    def disconnect(self):
        # gcs client does not require an explicit disconnect
        if self.service_account:
            os.remove(self.auth_file)

# Concrete type for FTP transfers
class FtpFileTransfer(FileTransfer):
    def __init__(self, connection_string, service_account:str = None):
        self.ftp = None
        self.conn_str = connection_string
    
    def connect(self):
        self.ftp = ftplib.FTP()
        # checking if a port was specified to connect
        if(self.conn_str.netloc.find(':') > -1):
            self.ftp.connect(self.conn_str.netloc.split(':')[0], int(self.conn_str.netloc.split(':')[1]))
        else:
            self.ftp.connect(self.conn_str.netloc)
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
        res = self.ftp.nlst(self.conn_str.path[:self.conn_str.path.rfind('/')])
        return fnmatch.filter(res, '*' + self.conn_str.path[self.conn_str.path.rfind('/'):])

# Concrete type for SFTP transfers
class SftpFileTransfer(FileTransfer):
    def __init__(self, connection_string, service_account:str = None):
        self.sftp = None
        self.conn_str = connection_string
    
    def connect(self):
        # parsing the query parameters to a dictionary for the login components
        auth_info = dict(parse.parse_qs(self.conn_str.query))
        # ignoring known_hosts
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        # checking if a port was specified to connect
        if(self.conn_str.netloc.find(':') > -1):
            self.sftp = pysftp.Connection(
                self.conn_str.netloc.split(':')[0], port=int(self.conn_str.netloc.split(':')[1]),
                username=auth_info['username'][0], password=auth_info['password'][0], cnopts=cnopts
            )
        else:
            self.sftp = pysftp.Connection(
                self.conn_str.netloc, username=auth_info['username'][0], password=auth_info['password'][0], cnopts=cnopts
            )
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
        folder_path = self.conn_str.path[:self.conn_str.path.rfind('/')]
        # listdir does not include the folder path, so we manually add it
        res = ['{}/{}'.format(folder_path, f) for f in self.sftp.listdir(folder_path)]
        return fnmatch.filter(res, '*' + self.conn_str.path[self.conn_str.path.rfind('/'):])

class S3FileTransfer(FileTransfer):
    """
    Concrete FileTransfer for S3 connections
    """
    def __init__(self, connection_string, project, config_file):
        FileTransfer.__init__(self, connection_string)
        self.config_file = config_file
        self.project = project
        self.s3 = None
        self.job_id = uuid.uuid4()
    
    def connect(self):
        self.auth_file = '/tmp/{0}/{0}.json'.format(self.job_id)
        client = storage.Client(self.project)

        with open(self.auth_file, 'wb') as f:
            client.download_blob_to_file(self.config_file, f)
            
        with open(self.auth_file, 'r') as fi:
            config = json.loads(fi.read())

        self.s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=config['access_key_id'],
            aws_secret_access_key=config['secret_access_key']
        )

        logging.info('Connected to S3 in profile: {}'.format(boto3.DEFAULT_SESSION.profile_name))
    
    def disconnect(self):
        logging.info('Disconnected from S3 in profile: {}'.format(boto3.DEFAULT_SESSION.profile_name))
    
    def download_file(self, file_path: str):
        # removing the leading / so as to not create a folder with it
        base_path = '{}/'.format(self.connection_string.path[1:self.connection_string.path.rfind('/')])
        base_path = base_path if base_path != '/' else ''

        file_name = file_path.split('/')[-1]
        dest_path = '/tmp/{}/{}'.format(self.job_id, file_name)

        with open(dest_path, 'wb') as f:
            self.s3.download_fileobj(self.connection_string.netloc, '{}{}'.format(base_path, file_name), f)

        logging.info('Downloaded file {} from S3 {} successfully'.format(file_name, self.connection_string.netloc))

        return dest_path
    
    def upload_file(self, file_path):
        # creating the final file path
        base_path = self.connection_string.path[1:]
        base_path = base_path if base_path.endswith('/') or base_path == '' else '{}/'.format(base_path)
        path = '{}{}'.format(base_path, file_path[file_path.rfind('/') + 1:])

        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(f, self.connection_string.netloc, path)

        logging.info('Uploaded file {} to S3 {} successfully'.format(file_path, self.connection_string.netloc))
    
    def remove_file(self, file_path):
        # creating the final file path
        self.s3.Object(self.connection_string.netloc, file_path).delete()
        
        logging.info('Removed file {} from S3 {} successfully'.format(file_path, self.connection_string.netloc))

    def list_files(self):
        folder_path = self.connection_string.path[:self.connection_string.path.rfind('/')]
        # listdir does not include the folder path, so we manually add it
        files = [f['Key'] for f in self.s3.list_objects_v2(Bucket=self.connection_string.netloc).get('Contents', [])]
        res = ['{}/{}'.format(folder_path, f) for f in files]
        match = fnmatch.filter(res, '*' + self.connection_string.path[self.connection_string.path.rfind('/'):])
        logging.info('Matched files: {}'.format(', '.join(match)))

        return match

def get_transfer_types():
    return {
        'ftp': FtpFileTransfer,
        'sftp': SftpFileTransfer,
        'gs': GcsFileTransfer,
        's3': S3FileTransfer
    }