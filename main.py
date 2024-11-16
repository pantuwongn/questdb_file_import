import argparse
import logging

from helpers import list_files, read_state, import_files_to_questdb
from questdb_helpers import get_tables, create_table

# Configuration
BASE_DIR = 'C:\\Users\\svc_gkcoop2\\Downloads\\NEWDATA'
QUESTDB_API_HOST = 'gkcoop2'
QUESTDB_API_PORT = '9000'
QUESTDB_API_EXEC_URL = f'http://{QUESTDB_API_HOST}:{QUESTDB_API_PORT}/exec'

# we will ingest files with BATCH__SIZE at once
BATCH_SIZE = 300  # from experiment, if we set more than this, it might cause buffer error
# Persistent state for last imported file per subfolder
STATE_FILE = 'state.json'

### LOG HANDLING ###

# Set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Formatter to include timestamp, filename, and message
formatter = logging.Formatter(
    '%(asctime)s - %(filename)s - %(levelname)s - %(message)s')

# Console handler for screen output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# File handler to log all messages at INFO level and above
file_handler = logging.FileHandler('import.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

####################


def main(delete_after_import=False, start_date=None, end_date=None):
    """Main entry point for importing files to QuestDB."""

    logger.info(f'========== Starting importing file with argument ({
                delete_after_import}, {start_date}, {end_date}) ==========')

    # read state file to determine last imported file per subfolder
    files, table_names = list_files(BASE_DIR, start_date, end_date)
    logger.info(f' Get files to be imported: {len(files)} files')

    if not files:
        logger.warning("No new files to import.")
        return

    # create table for all table_names
    current_tables = get_tables(QUESTDB_API_EXEC_URL)
    for table_name in table_names:
        if table_name not in current_tables:
            create_table(table_name, QUESTDB_API_EXEC_URL)
            logger.info(f'     Table, {table_name}, is created.')
        else:
            logger.info(f'     Table, {table_name}, already existed.')

    for i in range(0, len(files), BATCH_SIZE):
        batch = files[i:i + BATCH_SIZE]
        logger.info(f'  working on batch( {i}, {i+BATCH_SIZE})')
        import_files_to_questdb(
            batch, QUESTDB_API_HOST, QUESTDB_API_PORT, STATE_FILE, delete_after_import)
        logger.info(
            f"Imported batch {i // BATCH_SIZE + 1} of {len(files) // BATCH_SIZE + 1}")


if __name__ == "__main__":
    # define command-line arguments
    parser = argparse.ArgumentParser(
        description='Import files to QuestDB and optionally delete them afterwards.')

    parser.add_argument('--delete', action='store_true',
                        default=False, help='Delete files after successful import')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date for importing files (format: YYYY-MM-DD). If not specified, will import all file from the beginning.')
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date for importing files (format: YYYY-MM-DD). If not specified, will import until the latest file.')
    args = parser.parse_args()

    main(delete_after_import=args.delete,
         start_date=args.start_date, end_date=args.end_date)
