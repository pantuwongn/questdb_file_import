import argparse
import logging

from helpers import list_files, read_state, write_state

# Configuration
BASE_DIR = 'path/to/your/data/directory'
QUESTDB_API_URL = 'http://localhost:9000/exec'
BATCH_SIZE = 1000
IMPORT_LOG_FILE = 'import_log.txt'  # Tracks imported files to avoid duplicates
ERROR_LOG_FILE = 'error_log.txt'    # Logs files that encountered errors
# Persistent state for last imported file per subfolder
STATE_FILE = 'import_state.txt'

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
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

####################


def determine_format_type(file_path):
    """Determines the format type based on the file extension."""
    extension = file_path.suffix.lower()
    if extension == '.json':
        return 'json'
    elif extension == '.xml':
        return 'xml'
    elif extension == '.csv':
        return 'csv'
    else:
        return 'unknown'


def import_files_to_questdb(files, delete_after_import=False):
    """Imports a batch of files into QuestDB with error handling and optional deletion."""
    successfully_imported = []
    for file in files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                content = f.read()
                timestamp = parse_timestamp_from_filename(file.name)
                format_type = determine_format_type(file)

                data = {
                    'id': str(file),
                    'provider': file.parent.name,
                    'timestamp': timestamp,
                    'format_type': format_type,
                    'content': content
                }
                response = requests.post(QUESTDB_API_URL, data=f"""
                INSERT INTO raw_data VALUES (
                    '{data['id']}',
                    '{data['provider']}',
                    '{data['timestamp']}',
                    '{data['format_type']}',
                    '{data['content'].replace("'", "''")}'
                )
                """)
                if response.status_code != 200:
                    raise Exception(
                        f"Database insertion failed: {response.text}")
                successfully_imported.append(file)
                write_state(str(file.parent), str(file))
        except Exception as e:
            log_error(file, str(e))
            print(f"Error processing {file}: {e}")

    # Delete files if specified
    if delete_after_import:
        for file in successfully_imported:
            try:
                os.remove(file)
                print(f"Deleted: {file}")
            except OSError as e:
                log_error(file, f"Failed to delete {file}: {e}")


def main(delete_after_import=False, start_date=None, end_date=None):
    """Main entry point for importing files to QuestDB."""

    # read state file to determine last imported file per subfolder
    state = read_state(STATE_FILE)
    files = list_files(BASE_DIR, start_date, end_date)

    if not files:
        logger.warning("No new files to import.")
        return

    for i in range(0, len(files), BATCH_SIZE):
        batch = files[i:i + BATCH_SIZE]
        import_files_to_questdb(batch, delete_after_import)
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

    # main(delete_after_import=args.delete)
