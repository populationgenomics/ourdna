#!/usr/bin/env python3

import csv
import logging
from google.cloud import storage
import sys

BUCKET_NAME = 'cpg-mackenzie-main-upload'
SUMMARY_FILE = 'garvan-mm-summary.txt'
MANIFEST_SUFFIX = '/manifest.txt'
FILENAME_COLUMN = 'filename'
CHECKSUM_COLUMN = 'checksum'

def main():
    """Main entrypoint."""
    # Don't print DEBUG logs from urllib3.connectionpool.
    logging.getLogger().setLevel(logging.INFO)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)

    summary = bucket.get_blob(SUMMARY_FILE).download_as_string()
    any_mismatches = False
    for line in summary:
        # Skip anything that's not a manifest.
        if not line.endswith(MANIFEST_SUFFIX):
            continue

        # The summary lines contain the filename at the end, without any spaces.
        manifest_filename = line.split(' ')[-1]

        # Read the manifest (TSV).
        logging.info('reading manifest: {manifest_filename}')
        manifest = bucket.get_blob(manifest_filename).download_as_string()
        tsv_reader = csv.DictReader(manifest.splitlines(), delimiter='\t')
        for row in tsv_reader:
            # Filename is relative to the current directory.
            full_filename = manifest_filename.split('/')[:-1] + '/' + row[FILENAME_COLUMN]
            expected_md5 = row[CHECKSUM_COLUMN]

            # Read the checksum from the blob.
            check_blob = bucket.get_blob(full_filename)
            actual_md5 = check_blob.md5_hash

            if expected_md5 == actual_md5:
                logging.info(f'match: {full_filename}')
            else:
                logging.error(f'mismatch: {full_filename}, {expected_md5=}, {actual_md5=}')
                any_mismatches = True
    
    if any_mismatches:
        sys.exit(1)


if __name__ == '__main__':
    main()