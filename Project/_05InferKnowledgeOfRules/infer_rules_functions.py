import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import re
import datetime
import numpy as np
import json
from Project.Database import Db
from Project._04TPMAlgorithm.transform_for_TPM_algorithm import light_location_dict


def json_to_dataframe(year, level):
    json_file = json.load(
        open(Db.get_save_file_directory(f"output/NZERTF_year{year}_minsup0.14_minconf_0.5/level{level}.json")))
    if "," in json_file[0]["name_node"]:
        level_df = pd.DataFrame(columns=["pattern", "supp", "conf", "time"])
        for i in json_file:
            for j in i["patterns"]:
                level_df.loc[level_df.shape[0]] = j
    else:
        level_df = pd.DataFrame(columns=["name_node", "supp", "conf", "time"])
        for i in json_file:
            level_df.loc[level_df.shape[0]] = i
        level_df.rename(columns={'name_node': 'pattern'}, inplace=True)

    level_df = filter_rule_indexes(level_df)

    return level_df


# extractions_from_time_post_TPM


# Filter fules
def filter_rule_indexes(dataframe):
    meta = Db.load_data(meta=True, consumption=False, hourly=False)
    level_3_check = len(re.findall('\*', dataframe.loc[0, 'pattern'])) > 0
    rule_type = ['app_app_app', 'psn_app_app', 'psn_psn_app']
    dataframe['rule'] = pd.NA
    dataframe['multi_floor'] = pd.NA

    # filter follows rules in dataframe for level3: max 1 and level2: 0
    if level_3_check:
        dataframe = dataframe.loc[dataframe['pattern'].str.findall('-').map(len) <= 1]
    else:
        dataframe = dataframe.loc[dataframe['pattern'].str.findall('-').map(len) == 0]

    for index, row in dataframe.iterrows():
        tmp_floor_set = set()
        appliance_check_list = list()
        person_check_list = list()
        for col in set(re.findall('[\w_]+', row['pattern'])):
            person_check_list.append('SensHeat' in col)
            appliance_check_list.append('SensHeat' not in col)
            try:
                tmp_floor_set.add(meta.loc[col, 'Measurement_Floor'])
            except:
                tmp_floor_set.add(light_location_dict(meta)[col][0])

        if sum(appliance_check_list) >= 1:
            dataframe.loc[index, 'multi_floor'] = {'1stFloor', '2ndFloor'} == tmp_floor_set
            if {'1stFloor', '2ndFloor'} == tmp_floor_set:
                if level_3_check:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)]
                else:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)][:-4]
            else:
                if level_3_check:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)]
                else:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)][:-4]

    dataframe.dropna(inplace=True, axis=0)
    dataframe.reset_index(inplace=True, drop=True)
    return dataframe
