# archiveTree

This project provides tools for archiving a tree of files to an S3 bucket, and later retrieving the files.  

### Features:
- Support for Deep Glacier
- An inventory file is produced, which is later used for retrieval
- Large files stored as object, small files combined into tarballs.  Cutoff sizes can be specified
- Subtrees or individual files can be restored
- Checksums for all files are generated and (by default) checked on retrieval
- In verbose mode, tqdm provides progress bars
- Checksums, up- and down-loads are done in parallel

If an archive was stored in AWS Deep Glacier, an initial invocation of restore_from_s3.py will print a message that the data needs to be restored.
Restore_from_s3.py can then be run with --auto-request-restore, which will begin that process.  Later, list_object_status.py can be run to determine
if the data is ready to be downloaded, at which point restore_from_s3.py can be run again.

### Examples

To archive mydir:

```
python archive_to_s3.py --profile myprofile --verbose --storage-class DEEP_ARCHIVE /path/to/mydir mybucket .
```

To check the status of all objects:

```
python list_object_status.py --profile ycrcbjornson mydir.inventory.c25f55b2-aadd-4ce7-8c99-2099dfb541dd.json
```

To initiate a restore:

```
python restore_from_s3.py --profile myprofile --summary-csv restore.csv --restore-dir restored_mydir mydir.inventory.c25f55b2-aadd-4ce7-8c99-2099dfb541dd.json
```

### Requirements
The only required conda package is boto3.  tqdm is optional.
