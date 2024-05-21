#!/usr/bin/env python3

"""
Checks data transfer integrity by comparing MD5 checksums.

This script updates mackenzie_upload_md5_check.py, which was created for the
first Mackenzie transfer in February 2023. This script simplifies the original,
as only md5 checksums from a single folder need to be verified.
"""

import base64
import binascii
import csv
import logging
import sys
from google.cloud import storage

BUCKET_NAME = 'cpg-mackenzie-main-upload'
MANIFEST_FILENAME = 'NSWHP/2024-05-21/manifest.txt'
MANIFEST_DIR = 'NSWHP/2024-05-21/'
FILENAME_COLUMN = 'filename'
CHECKSUM_COLUMN = 'checksum'


def main():
    """Main entrypoint."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    any_errors = False

    # Read the manifest (TSV).
    logging.info(f'reading manifest: {MANIFEST_FILENAME}')
    manifest_blob = bucket.get_blob(MANIFEST_FILENAME)
    if manifest_blob:
        manifest = manifest_blob.download_as_text()

        # Check every file listed in the manifest.
        # Filenames are relative to the manifest directory.
        tsv_reader = csv.DictReader(manifest.splitlines(), delimiter='\t')
        for row in tsv_reader:
            full_filename = MANIFEST_DIR + row[FILENAME_COLUMN]
            expected_md5 = row[CHECKSUM_COLUMN]

            # Read the checksum from the blob. The checksum is base64-encoded.
            check_blob = bucket.get_blob(full_filename)
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
