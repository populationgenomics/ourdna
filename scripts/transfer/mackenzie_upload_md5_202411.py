#!/usr/bin/env python3

import base64
import binascii
import csv
import logging
import sys

import click
from google.cloud import storage

"""
Checks data transfer integrity by comparing MD5 checksums.

This script tweaks mackenzie_upload_md5_202405.py to align with locations of manifests and uploaded files.
Usage:
    python mackenzie_upload_md5_202411.py \
    --upload-bucket cpg-mackenzie-main-upload \
    --transfer-folder VCGS/2024_11_transfer/ \
    --manifest-filename VCGS/2024_11_transfer/cpg_fastq_metadata.csv \
    --manifest-bucket cpg-mackenzie-test-upload \
    --filename-column filename \
    --checksum-column checksum

Options:
    --upload-bucket TEXT       The bucket where files to check are stored.
    --transfer-folder TEXT     The folder suffix where files to check are stored.
    --manifest-filename TEXT   The manifest file to read.
    --manifest-bucket TEXT     The directory where manifest is stored 
                               (i.e. manifest may be in test bucket while file uploads are in main).
    --filename-column TEXT     The column in the manifest with the filename.
    --checksum-column TEXT     The column in the manifest with the checksum.
"""


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
            full_filename = upload_bucket.name + transfer_folder + row[filename_column]
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
