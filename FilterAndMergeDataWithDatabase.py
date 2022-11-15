import logging
import os

import pandas as pd
from sqlalchemy import create_engine, null
from sqlalchemy_utils import database_exists, create_database

# remember to truncate related tables if exist before running this script
data_set_1_url = "https://data.glygen.org/ln2data/releases/data/v-2.0.1/reviewed/human_protein_info_uniprotkb.csv"
data_set_2_url = "https://data.glygen.org/ln2data/releases/data/v-2.0.1/reviewed" \
                 "/human_proteoform_glycosylation_sites_unicarbkb.csv "

try:
    iter_csv_1 = pd.read_csv(data_set_1_url,
                             usecols=['uniprotkb_canonical_ac', 'uniprotkb_protein_mass', 'uniprotkb_protein_length'],
                             iterator=True, chunksize=10000)

    logging.info("iter_csv: " + str(iter_csv_1))

    iter_csv_2 = pd.read_csv(data_set_2_url,
                             usecols=['uniprotkb_canonical_ac', 'glycosylation_site_uniprotkb', 'amino_acid',
                                      'saccharide', 'glycosylation_type', 'xref_key', 'xref_id'],
                             iterator=True, chunksize=10000)

    user = 'root'
    password = '12345'
    host = '127.0.0.1'
    port = 3306
    database = 'CombineData'
    tableName = "human_protein_info_uniprotkb"
    tableName2 = "human_proteoform_glycosylation_sites_unicarbkb"

    # python function to connect to the mysql database and return the sqlachemy engine object
    def get_connection():
        url = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
        if not database_exists(url):
            create_database(url)
        return create_engine(url)

    sqlEngine = get_connection()

    dbConnection = sqlEngine.connect().execution_options(
        stream_results=True)

    try:
        df = None
        for chunk in iter_csv_1:
            dataFrame = pd.DataFrame(data=chunk)

            # Write records stored in a DataFrame to a SQL database.
            logging.info(chunk)
            print(chunk)
            df_filtered = chunk.loc[chunk["uniprotkb_protein_mass"] > 10000]
            frame = df_filtered.to_sql(tableName, dbConnection, if_exists='append')
        print(f'ALTER TABLE {tableName} ADD PRIMARY KEY (uniprotkb_canonical_ac)')

        for chunk2 in iter_csv_2:
            logging.info(f'chunk2: {chunk2}')
            dataFrame = pd.DataFrame(data=chunk2)
            frame = dataFrame.to_sql(tableName2, dbConnection, if_exists='append')

        stmt = f'select {tableName}.uniprotkb_canonical_ac, {tableName}.uniprotkb_protein_mass, {tableName}.uniprotkb_protein_length, {tableName2}.glycosylation_site_uniprotkb, {tableName2}.amino_acid, {tableName2}.saccharide, {tableName2}.glycosylation_type, {tableName2}.xref_key, {tableName2}.xref_id from {tableName} JOIN {tableName2} ON {tableName}.uniprotkb_canonical_ac = {tableName2}.uniprotkb_canonical_ac;'
        print(stmt)

        for chunk_dataframe in pd.read_sql(stmt, dbConnection, chunksize=1000):
            if not os.path.isfile('./human_proteoform_glycosylation_sites_unicarbkb_heavyproteins.csv'):
                chunk_dataframe.to_csv('./human_proteoform_glycosylation_sites_unicarbkb_heavyproteins.csv')
            else:
                chunk_dataframe.to_csv("./human_proteoform_glycosylation_sites_unicarbkb_heavyproteins.csv", mode='a',
                                       header=False)
        print(f'type: {type(chunk_dataframe)}')

    except Exception as ex:
        logging.error(str(ex))

    finally:
        if dbConnection != null:
            dbConnection.close()

except Exception as ex:
    logging.error(str(ex))
