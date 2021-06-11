# Cloud Function - File transference

Function that transfers, one by one, files in one location to another (i.e. copies files from an FTP to a GCS bucket).
The function is based on a Pub/Sub message which must be a JSON in the following format:
```
{
    "source_connection_string": "ftp://FTP/TEMP?username=user&password=pass",
    "destination_connection_string": "gs://BUCKET/temp/",
    "remove_file": False,
    "compress_algorithm": "zip",
    "decompress_algorithm": "zip",
    "event_date": "2020-07-01T23:00:00+00:00",
    "service_account": "gs://BUCKET/service-account-json.json"
}
```
The `remove_file` attribute determines whether or not the file should be removed from the source if it is successfully copied to the destination.

The attributes `compress_algorithm` and `decompress_algorithm` determine that the file must be zipped or unzipped, respectively, before being sent to the destination.

The `event_date` attribute, if provided, will be used to terminate retries after 1 hour of failures (GCF terminates after 7 days of attempts).

The `service_account` attribute, if provided, will be used to instantiate GCS clients using another GCP service account.
It is only used for GCS and if the attribute is not provided it will use the service account defined in the GCF.

Connection strings must follow the URI pattern. We currently support the following connections:

* FTP - `ftp://HOSTNAME/PATH/FILE?username=USERNAME&password=PASSWORD`
* FTPS - `ftps://HOSTNAME/PATH/FILE?username=USERNAME&password=PASSWORD`
* SFTP - `sftp://HOSTNAME/PATH/FILE?username=USERNAME&password=PASSWORD`
* GCS - `gs://BUCKET/PATH/`

The currently supported compression types are as follows:

* gzip
* zip

**ATTENTION**: the compressed files received as source must, necessarily, contain only one file. Likewise, each file sent to the destination will be zipped into its own file.

To give a minimum standard of consistency, the file listing methods used internally list all files at the level of the last part of the PATH and then filter via [fnmatch](https://docs.python.org/3.4/library/fnmatch.html) in the final part.

For example, let's assume a PATH = `/FILES/*_log.txt`. First we list all files in `/FILES/`. In this list, we filter `*_log.txt`. With this, at least at the last level we will have the same behavior in all connection types (ie this reduces the inconsistency in the fact that an FTP connection accepts *wildcards* anywhere, whereas GCS only allows it to have one prefix ).

**ATTENTION**: no validation is done on the path to the directory. That is, a request like `/FILES/*/*_log.txt` will work for FTP connections but not GCS. Keep in mind that the function is only recommended on relatively small volumes of data/files.

## Environment Variables

* **PROJECT** = Project ID (ex: modular-aileron-191222)

## Deployment

Publish as Google Cloud Function - environment Python 3.7 with the above variables. Use a PubSub message trigger.

## Built With

* [Python](https://www.python.org/) - Runtime Environment