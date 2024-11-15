import logging
import os
import re

from datetime import datetime, timedelta

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

# Regex pattern to extract timestamp from the filename (e.g., data_20231101_153000.txt)
TIMESTAMP_PATTERN = re.compile(r'(\d{8}_\d{6})')


def parse_timestamp_from_filename(filename):
    """Parses the timestamp from the filename and converts it to a datetime object."""

    match = TIMESTAMP_PATTERN.search(filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d_%H%M%S')
    else:
        raise ValueError("Timestamp not found or invalid in filename")


def list_files(base_dir, start_date: str = None, end_date: str = None) -> list[tuple[str,str,str]]:
    """Lists files in a directory and its subdirectories, filtering by date range.
        RETURN: list of tuple, each is filename, filepath, prefix(table name)
    """

    # parse starte_date and end_date to datetime objects
    # NOTE: format of start_date and end_date should be 'YYYY-MM-DD'
    start_date = datetime.strptime(
        start_date, '%Y-%m-%d') if start_date else None
    end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else None

    files_to_import = []
    prefixes = set()
    for root, _, files in os.walk(base_dir):
        for f in files:
            # get datetime from filename
            # if the filename does not contain a valid timestamp, skip the file
            try:
                dt = parse_timestamp_from_filename(f)
            except ValueError:
                continue

            # extract prefix by spliting using _
            prefix = f.split('_')[0]

            # construct full path of the file
            fp = os.path.join(root, f)

            if start_date and end_date:
                if start_date <= dt <= end_date:
                    files_to_import.append( (f, fp, prefix) )
            elif start_date:
                if dt >= start_date:
                    files_to_import.append( (f, fp, prefix) )
            elif end_date:
                if dt <= end_date:
                    files_to_import.append( (f, fp, prefix) )
            else:
                files_to_import.append( (f, fp, prefix) )

    return files_to_import, list(prefixes)


def read_state(state_file_path: str = 'state.txt') -> dict[str, str]:
    """Reads the state file to determine the last imported file for each subfolder."""
    state = {}
    if os.path.exists(state_file_path):
        with open(state_file_path, 'r') as state_file:
            for line in state_file:
                subfolder, last_file = line.strip().split('=', 1)
                state[subfolder] = last_file
    return state


def write_state(subfolder, last_file, state_file_path: str = 'state.txt'):
    """Writes the last imported file for a subfolder to the state file."""
    state = read_state()
    state[subfolder] = last_file
    with open(state_file_path, 'w') as state_file:
        for sub, file in state.items():
            state_file.write(f"{sub}={file}\n")


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
