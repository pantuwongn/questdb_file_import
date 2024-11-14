import os
import re

from datetime import datetime, timedelta

# Regex pattern to extract timestamp from the filename (e.g., data_20231101_153000.txt)
TIMESTAMP_PATTERN = re.compile(r'(\d{8}_\d{6})')


def parse_timestamp_from_filename(filename):
    """Parses the timestamp from the filename and converts it to a datetime object."""

    match = TIMESTAMP_PATTERN.search(filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d_%H%M%S')
    else:
        raise ValueError("Timestamp not found or invalid in filename")


def list_files(base_dir, start_date: str = None, end_date: str = None) -> tuple[list[str], list[str]]:
    """Lists files in a directory and its subdirectories, filtering by date range."""

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
            prefixes.add(prefix)

            # construct full path of the file
            f = os.path.join(root, f)

            if start_date and end_date:
                if start_date <= dt <= end_date:
                    files_to_import.append(f)
            elif start_date:
                if dt >= start_date:
                    files_to_import.append(f)
            elif end_date:
                if dt <= end_date:
                    files_to_import.append(f)
            else:
                files_to_import.append(f)

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
