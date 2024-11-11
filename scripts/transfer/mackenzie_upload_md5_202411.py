#!/usr/bin/env python3

"""
Checks data transfer integrity by comparing MD5 checksums.

This script updates mackenzie_upload_md5_check.py, which was created for the
first Mackenzie transfer in February 2023. This script simplifies the original,
as only md5 checksums from a single folder need to be verified. This script was
executed on May 22, 2024.
"""

import base64
import binascii
import csv
import logging
import sys

import click
from google.cloud import storage

UPLOAD_BUCKET = 'cpg-mackenzie-main-upload'
transfer_folder = 'VCGS/2024_11_transfer/'
manifest_filename = 'VCGS/2024_11_transfer/cpg_fastq_metadata.csv'
manifest_bucket = 'cpg-mackenzie-test-upload'
filename_column = 'filename'
checksum_column = 'checksum'

@click.command()
@click.option('--upload-bucket', help='The bucket where files to check are stored.')
@click.option('--transfer-folder', help='The folder where files to check are stored.')
@click.option('--manifest-filename', help='The manifest file to read.')
@click.option('--manifest-bucket', help='The directory where manifest is stored')
@click.option('--filename-column', help='The column in the manifest with the filename.')
@click.option('--checksum-column', help='The column in the manifest with the checksum.')
def main(upload_bucket, transfer_folder, manifest_filename, manifest_bucket, filename_column, checksum_column):
    """Main entrypoint."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )

    storage_client = storage.Client()
    upload_bucket = storage_client.get_bucket(upload_bucket)
    upload_file_blobs = list(upload_bucket.list_blobs(prefix=transfer_folder))
    manifest_bucket = storage_client.get_bucket(manifest_bucket)

    # Create a dictionary of upload blobs by filename.
    upload_file_blob_dict = {blob.name.split('/')[-1]: blob for blob in upload_file_blobs}

    any_errors = False

    # Read the manifest (TSV).
    logging.info(f'reading manifest: {manifest_filename}')
    manifest_blob = manifest_bucket.get_blob(manifest_filename)
    if manifest_blob:
        manifest = manifest_blob.download_as_text()
        # Remove BOM if present
        if manifest.startswith('\ufeff'):
            manifest = manifest[1:]

        # Check every file listed in the manifest.
        # Filenames are relative to the manifest directory.
        tsv_reader = csv.DictReader(manifest.splitlines())
        for row in tsv_reader:
            full_filename = UPLOAD_BUCKET + transfer_folder + row[filename_column]
            expected_md5 = row[checksum_column]

            # Read the checksum from the blob. The checksum is base64-encoded.
            check_blob = upload_file_blob_dict.get(row[filename_column])
            if not check_blob:
                logging.error(f'blob does not exist: {full_filename}')
                any_errors = True
                continue
            actual_md5 = binascii.hexlify(
                base64.urlsafe_b64decode(check_blob.md5_hash)
            ).decode('utf-8')

            if expected_md5 == actual_md5:
                logging.info(f'match: {full_filename}')
            else:
                logging.error(
                    f'mismatch: {full_filename}, {expected_md5=}, {actual_md5=}'
                )
                any_errors = True

    if any_errors:
        sys.exit(1)


if __name__ == '__main__':
    main()
