import logging
import requests

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


def create_table(table_name: str, questdb_url: str):
    """ This function create a table on QuestDB """
    sql_statement = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id STRING,
            provider STRING,
            format STRING,
            timestamp TIMESTAMP,
            content STRING
        )
        TIMESTAMP(timestamp)
        PARTITION BY DAY
    """
    try:
        params = {
            'query': sql_statement,
            'count': 'true',
        }

        response = requests.get(questdb_url, params=params)

        if response.status_code == 200:
            return True
        else:
            logger.error(f'Cannot create table because of {response.text}')
            return False
    except Exception as e:
        logger.exception(f'Cannot create table because of {e!r}')
        return False


def get_tables(questdb_url: str):
    """ This function returns all table name on QuestDB """
    sql_statement = 'tables();'
    try:
        params = {
            'query': sql_statement,
            'count': 'true',
        }

        response = requests.get(questdb_url, params=params)

        if response.status_code == 200:
            tables = [d[1] for d in response.json()['dataset']]
            return tables
        else:
            logger.error(f'Cannot get table because of {response.text}')
            return []
    except Exception as e:
        logger.exception(f'Cannot get table because of {e!r}')
        return []
