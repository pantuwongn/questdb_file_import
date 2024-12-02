import logging
import json
import os
import numpy as np
import pandas as pd
import pytz
import re
import time

from datetime import datetime, timedelta
from questdb_helpers import import_csv

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

# Regex pattern to extract timestamp from the filename (e.g., data_20231101_153000.txt)
TIMESTAMP_PATTERN = re.compile(r'(\d{8}-\d{6})')


def parse_timestamp_from_filename(filename):
    """Parses the timestamp from the filename and converts it to a datetime object."""

    match = TIMESTAMP_PATTERN.search(filename)
    window_tick_match = re.search(r"_(\d+)\.xml$", filename)

    if match:
        dt = datetime.strptime(match.group(1), '%Y%m%d-%H%M%S')
        # Define Eastern Time (ET) timezone
        eastern = pytz.timezone('US/Eastern')

        # Localize the datetime object to Eastern Time
        localized_dt = eastern.localize(dt)
        return localized_dt
    elif window_tick_match:
        windows_tick = int(window_tick_match.group(1))
        base_date = datetime(1, 1, 1)
        dt = base_date + timedelta(microseconds=windows_tick // 10)
        # Define Eastern Time (ET) timezone
        eastern = pytz.timezone('US/Eastern')

        # Localize the datetime object to Eastern Time
        localized_dt = eastern.localize(dt)
        return localized_dt
    else:
        raise ValueError("Timestamp not found or invalid in filename")


def list_files(base_dir, start_date: str = None, end_date: str = None):
    """Lists files in a directory and its subdirectories, filtering by date range.
        RETURN: list of tuple, each is filename, filepath, prefix(table name)
    """

    # parse starte_date and end_date to datetime objects
    # NOTE: format of start_date and end_date should be 'YYYY-MM-DD'
    start_date = datetime.strptime(
        start_date, '%Y-%m-%d') if start_date else None
    end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else None

    # Define Eastern Time (ET) timezone
    eastern = pytz.timezone('US/Eastern')
    if start_date:
        start_date = eastern.localize(start_date)
    if end_date:
        end_date = eastern.localize(end_date)

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
            fp = os.path.join(root, f)

            if start_date and end_date:
                if start_date <= dt <= end_date:
                    files_to_import.append((f, fp, prefix))
            elif start_date:
                if dt >= start_date:
                    files_to_import.append((f, fp, prefix))
            elif end_date:
                if dt <= end_date:
                    files_to_import.append((f, fp, prefix))
            else:
                files_to_import.append((f, fp, prefix))

    return files_to_import, list(prefixes)


def read_state(state_file_path: str = 'state.json'):
    """Reads the state file to determine the last imported file for each subfolder."""
    state = {}
    if os.path.exists(state_file_path):
        with open(state_file_path, 'r') as state_file:
            json.load(state_file)
    return state


def write_state(state_data, state_file_path: str = 'state.json'):
    """Writes the last imported file for a subfolder to the state file."""
    with open(state_file_path, 'w') as state_file:
        json.dump(state_data, state_file)


def determine_format_type(file_path):
    """Determines the format type based on the file extension."""

    _, extension = os.path.splitext(file_path)

    if extension == '.json':
        return 'json'
    elif extension == '.xml':
        return 'xml'
    elif extension == '.csv':
        return 'csv'
    else:
        return 'unknown'


def import_files_to_questdb(files, questdb_url, state_file: str,
                            delete_after_import=False, max_retry: int = 50):
    """Imports a batch of files into QuestDB with error handling and optional deletion."""

    file_to_be_deleted = {}
    filename_import = {}
    state = read_state(state_file)
    logger.info(f' Read state: {state}')

    logger.info('        Constructing dataframe to import to QuestDB')
    dfDict = {}
    for file in files:
        filename = file[0]
        filepath = file[1]
        prefix = file[2]

        # check from state file
        if prefix not in state:
            state[prefix] = []

        if filename in state[prefix]:
            logger.warning(f'       {filename} has already imported, skip!!')
            continue

        if prefix not in dfDict:
            df = pd.DataFrame(
                columns=['id', 'provider', 'timestamp', 'format', 'content'])
            dfDict[prefix] = df

        if prefix not in file_to_be_deleted:
            file_to_be_deleted[prefix] = []
            filename_import[prefix] = []

        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            try:
                timestamp = parse_timestamp_from_filename(filename)
            except ValueError:
                logger.error(f'        cannot import filename = \
                             {filename} because cannot get timestamp from filename!!!!')
                continue
            format_type = determine_format_type(filepath)
            if format_type == 'unknown':
                logger.warning(
                    f'       {filename} has unsupported format, skip!!')
                continue

            dfDict[prefix].loc[len(df)] = [str(
                filename), prefix, timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), format_type, content]

            file_to_be_deleted[prefix].append(filepath)
            filename_import[prefix].append(filename)

    logger.info(f'        Dataframe is created with \
                {len(df)} rows, input contains {len(files)} files')
    # import with python client
    for prefix in dfDict:
        retry = 0
        success = False
        while not success and retry < max_retry:
            df = dfDict[prefix]
            df.to_csv('tmp_data.csv', index=False)
            success = import_csv('tmp_data.csv', prefix, questdb_url)
            if not success:
                time.sleep(10)
                retry += 1
        os.remove('tmp_data.csv')
        if success and prefix in file_to_be_deleted:
            if prefix not in state:
                state[prefix] = filename_import[prefix]
            else:
                state[prefix].extend(filename_import[prefix])
            if delete_after_import:
                for filepath in file_to_be_deleted[prefix]:
                    try:
                        os.remove(filepath)
                    except OSError as e:
                        logger.warning(
                            f"       Failed to delete {file}: {e}")
            write_state(state, state_file)
            logger.info(
                f"       Complete insert files for {prefix} to QuestDB")
        else:
            logger.error(
                f"       Fail to insert files for {prefix} to QuestDB. Please check the log")
