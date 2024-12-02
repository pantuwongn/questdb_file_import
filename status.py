from questdb_helpers import get_blob_tables, get_min_max_timestamp

# Configuration
QUESTDB_API_HOST = 'gkcoop2'
QUESTDB_API_PORT = '9000'
QUESTDB_API_EXEC_URL = f'http://{QUESTDB_API_HOST}:{QUESTDB_API_PORT}/exec'


def main():

    # info
    info_list = []

    # get all tables from QuestDB
    current_tables = get_blob_tables(QUESTDB_API_EXEC_URL)

    # for each table name, check its schema
    for table_name in current_tables:
        # get min,max timestamp
        min_time, max_time = get_min_max_timestamp(
            QUESTDB_API_EXEC_URL, table_name)

        info_list.append((table_name, min_time, max_time))

    # display table
    print("{:<15} {:<30} {:<30}".format(
        'Table name', 'Min timestamp', 'Max timestamp'))

    # print each data item.
    for table_name, min_time, max_time in info_list:
        print("{:<15} {:<30} {:<30}".format(table_name, min_time, max_time))


if __name__ == '__main__':
    main()
