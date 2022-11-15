import os

import pandas as pd
import logging

data_set_1_url = "https://data.glygen.org/ln2data/releases/data/v-2.0.1/reviewed/human_protein_info_uniprotkb.csv"
data_set_2_url = "https://data.glygen.org/ln2data/releases/data/v-2.0.1/reviewed/human_proteoform_glycosylation_sites_unicarbkb.csv"

try:
    iter_csv_1 = pd.read_csv(data_set_1_url,
                             usecols=['uniprotkb_canonical_ac', 'uniprotkb_protein_mass', 'uniprotkb_protein_length'],
                             iterator=True, chunksize=10000)

    logging.info("iter_csv_1" + str(iter_csv_1))

    df = None
    for chunk in iter_csv_1:
        logging.info(type(chunk))
        print(chunk)
        df1 = chunk.loc[chunk["uniprotkb_protein_mass"] > 10000]
        print(f'uniprotkb_canonical_ac with Q9Y6M5 : {df1.loc[df1["uniprotkb_canonical_ac"] == "Q9Y6M5-1"]}')
        iter_csv_2 = pd.read_csv(data_set_2_url,
                                 usecols=['uniprotkb_canonical_ac', 'glycosylation_site_uniprotkb', 'amino_acid',
                                          'saccharide', 'glycosylation_type', 'xref_key', 'xref_id'],
                                 iterator=True, chunksize=10000)
        for chunk2 in iter_csv_2:
            print(f'chunk2: {chunk2}')
            print(
                f' chunk2 uniprotkb_canonical_ac equal to Q9Y6M5?? : {chunk2.loc[chunk2["uniprotkb_canonical_ac"] == "Q9Y6M5-1"]}')

            df_merged = pd.merge(df1, chunk2, on="uniprotkb_canonical_ac")
            print(f'df_merged:  {df_merged}')
            print(f' df merged uniprotkb_canonical_ac equal to Q9Y6M5?? : {df_merged.loc[df_merged["uniprotkb_canonical_ac"] == "Q9Y6M5-1"]}')

            # avoid writing to csv header multiple times
            if not os.path.isfile('./output/human_proteoform_glycosylation_sites_unicarbkb_heavyproteins.csv'):
                df_merged.to_csv('./output/human_proteoform_glycosylation_sites_unicarbkb_heavyproteins.csv')
            else:
                df_merged.to_csv('./output/human_proteoform_glycosylation_sites_unicarbkb_heavyproteins.csv', mode='a', header=False)

except Exception as ex:
    logging.exception(str(ex))
