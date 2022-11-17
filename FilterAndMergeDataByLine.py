import csv
import os
import urllib.request
from os import path

import chardet

data_set_1_url = "https://data.glygen.org/ln2data/releases/data/v-2.0.1/reviewed/human_protein_info_uniprotkb.csv"
data_set_2_url = "https://data.glygen.org/ln2data/releases/data/v-2.0.1/reviewed" \
                 "/human_proteoform_glycosylation_sites_unicarbkb.csv "

needed_cols_data_set_1 = ['uniprotkb_canonical_ac', 'uniprotkb_protein_mass', 'uniprotkb_protein_length']
needed_cols_data_set_2 = ["uniprotkb_canonical_ac", "glycosylation_site_uniprotkb", "amino_acid", "saccharide",
                          "glycosylation_type", "xref_key", "xref_id"]
indexes_needed_1 = []
indexes_needed_2 = []

is_header_column = True
is_header_column2 = True

try:
    with urllib.request.urlopen(data_set_1_url) as response1:
        visited_uniprotkb_canonical_ac = set()
        for line in response1:
            print(f'Enter new for loop entry')
            the_encoding = chardet.detect(line)['encoding']
            # convert row string to column list
            row = line.decode(the_encoding).strip().replace('"', '').split(",")
            # print(f'row: {row}')
            if is_header_column:
                for col in needed_cols_data_set_1:
                    print(f'header row 0: {row[0]}')
                    print(f'header row: {row}')
                    indexes_needed_1.append(row.index(col))
                    print(f'indexes needed 1 : {indexes_needed_1}')
                is_header_column = False
            # skip line with uniprotkb_protein_mass <= 100000 to reduce time complexity
            if row[2].isdigit() and int(row[2]) <= 10000:
                print(f'skipped line cause of {row[2]} mass <= 10000')
                continue

            with urllib.request.urlopen(data_set_2_url) as response2:
                for line2 in response2:
                    the_encoding2 = chardet.detect(line2)['encoding']
                    row2 = line2.decode(the_encoding2).strip().replace('"', '').split(",")
                    # print(f'row: {row}')
                    if is_header_column2:
                        for col2 in needed_cols_data_set_2:
                            print(f'header row 0: {row2[0]}')
                            print(f'header row: {row2}')
                            indexes_needed_2.append(row2.index(col2))
                            print(f'indexes needed 2 : {indexes_needed_2}')
                        is_header_column2 = False
                    # print(f'row2 : {row2}')

                    output_file_name = "human_proteoform_glycosylation_sites_unicarbkb_heavyproteins.csv"

                    if not path.exists("./outputMergeByLine"):
                        os.mkdir("./outputMergeByLine")

                    if row[0] == 'uniprotkb_canonical_ac' and row2[0] == 'uniprotkb_canonical_ac':
                        with open("./outputMergeByLine" + "/" + output_file_name, 'a+', newline='') as file_data:
                            writer = csv.writer(file_data, delimiter=',')
                            print(f'needed cols2 part: {needed_cols_data_set_2[1:]}')
                            header_row_merged = needed_cols_data_set_1 + needed_cols_data_set_2[1:]
                            print(f'header row merged: {header_row_merged}')
                            writer.writerow(header_row_merged)
                            break

                    if row[0] == 'uniprotkb_canonical_ac' or row2[0] == 'uniprotkb_canonical_ac' or row2[0] in visited_uniprotkb_canonical_ac:
                        continue

                    if row[0] == row2[0]:
                        with open("./outputMergeByLine" + "/" + output_file_name, 'a+', newline='') as file_data:
                            writer = csv.writer(file_data, delimiter=',')
                            row_merged = []
                            for id_1 in indexes_needed_1:
                                row_merged.append(row[id_1])

                            for id_2 in indexes_needed_2[1:]:
                                row_merged.append(row2[id_2])
                            print(f'row_merged: {row_merged}')
                            writer.writerow(row_merged)

            visited_uniprotkb_canonical_ac.add(row[0])
except Exception as ex:
    print(str(ex))
