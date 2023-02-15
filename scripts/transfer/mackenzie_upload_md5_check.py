#!/usr/bin/env python3

"""Checks data transfer integrity by comparing MD5 checksums."""

import csv
import logging
import sys
from google.cloud import storage

BUCKET_NAME = 'cpg-mackenzie-main-upload'
SUMMARY_FILE = 'garvan-mm-summary.txt'
MANIFEST_SUFFIX = '/manifest.txt'
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

    summary_blob = bucket.get_blob(SUMMARY_FILE)
    if not summary_blob:
        logging.error(f'blob does not exist: {SUMMARY_FILE}')
        sys.exit(1)
    summary = summary_blob.download_as_text()

    any_errors = False
    for line in summary.splitlines():
        # Skip anything that's not a manifest.
        if not line.endswith(MANIFEST_SUFFIX):
            continue

        # The summary lines contain the filename at the end, without any spaces.
        manifest_filename = line.split(' ')[-1]

        # Read the manifest (TSV).
        logging.info('reading manifest: {manifest_filename}')
        manifest_blob = bucket.get_blob(manifest_filename)
        if not manifest_blob:
            logging.error(f'blob does not exist: {manifest_filename}')
            any_errors = True
            continue
        manifest = manifest_blob.download_as_text()

        # Check every file listed in the manifest.
        # Filenames are relative to the manifest directory.
        manifest_dir = manifest_filename[:manifest_filename.rfind('/') + 1]
        tsv_reader = csv.DictReader(manifest.splitlines(), delimiter='\t')
        for row in tsv_reader:
            full_filename = manifest_dir + row[FILENAME_COLUMN]
            expected_md5 = row[CHECKSUM_COLUMN]

            # Read the checksum from the blob.
            check_blob = bucket.get_blob(full_filename)
            if not check_blob:
                logging.error(f'blob does not exist: {full_filename}')
                any_errors = True
                continue
            actual_md5 = check_blob.md5_hash

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
