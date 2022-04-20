import json
import re

import numpy as np
import pandas as pd

from Project.Database import Db
from Project._04TPMAlgorithm.transform_for_TPM_algorithm import light_location_dict


def json_to_dataframe(year, level, exclude_follows=True, with_redundancy=True):
    redundancy = '' if with_redundancy else '_no_redundancy'
    json_file = json.load(
        open(Db.get_save_file_directory(f"output/NZERTF_year{year}{redundancy}_minsup0.14_minconf_0.5/level{level}.json")))
    if "," in json_file[0]["name_node"]:
        level_df = pd.DataFrame(columns=["pattern", "supp", "conf", "time"])
        level1=False
        for i in json_file:
            for j in i["patterns"]:
                level_df.loc[level_df.shape[0]] = j
    else:
        level_df = pd.DataFrame(columns=["name_node", "supp", "conf", "time"])
        for i in json_file:
            level_df.loc[level_df.shape[0]] = i
        level_df.rename(columns={'name_node': 'pattern'}, inplace=True)
        level1=True

    level_df = filter_rule_indexes(level_df, level1, exclude_follows=exclude_follows)

    return level_df


# extractions_from_time_post_TPM


# Filter fules
def filter_rule_indexes(dataframe, level1, exclude_follows=True):
    meta = Db.load_data(meta=True, consumption=False, hourly=False)
    level_3_check = len(re.findall('\*', dataframe.loc[0, 'pattern'])) > 0
    rule_type = ['app_app_app', 'psn_app_app', 'psn_psn_app']
    dataframe['rule'] = pd.NA
    dataframe['multi_floor'] = pd.NA
    dataframe['floor'] = pd.NA

    # filter follows rules in dataframe for level3: max 1 and level2: 0
    if exclude_follows:
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
                tmp_floor_set.add(meta.loc[light_location_dict(meta)[col][0], 'Measurement_Floor'])

        # Filter rules that has at least one appliance or is level 1
        dataframe.loc[index, 'multi_floor'] = {'1stFloor', '2ndFloor'} == tmp_floor_set
        if sum(appliance_check_list) >= 1 and not level1:
            if {'1stFloor', '2ndFloor'} == tmp_floor_set:
                dataframe.loc[index, 'floor'] = 'multi'
                if level_3_check:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)]
                else:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)][:-4]
            else:
                dataframe.loc[index, 'floor'] = list(tmp_floor_set)[0]
                if level_3_check:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)]
                else:
                    dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)][:-4]
        elif level1:
            dataframe.loc[index, 'floor'] = list(tmp_floor_set)[0]
            dataframe.loc[index, 'rule'] = rule_type[sum(person_check_list)][:-8]


    dataframe.dropna(inplace=True, axis=0)
    dataframe.reset_index(inplace=True, drop=True)
    return dataframe


def start_end_times_of_rules(dictionary):
    start_end_list = []
    start_end_set_list = []
    for day in dictionary.values():
        start_end_set = set()
        for event in day:
            try:
                event_start_times = []
                event_end_times = []
                for appliance in event:
                    event_start_times.append(appliance[0])
                    event_end_times.append(appliance[1])
                start_time = int(min(event_start_times).split(":")[0].split(" ")[1])
                end_time = int(max(event_end_times).split(":")[0].split(" ")[1])
                start_end_list.append([start_time] + [end_time])
                start_end_set.update({hour for hour in range(start_time, end_time + 1)})
            except:
                start_time = int(event[0].split(":")[0].split(" ")[1])
                end_time = int(event[1].split(":")[0].split(" ")[1])
                start_end_list.append([start_time] + [end_time])
                start_end_set.update({hour for hour in range(start_time, end_time + 1)})
        start_end_set_list.append(list(start_end_set))

    temp_df = pd.DataFrame(start_end_list, columns=['start_time', 'end_time'])
    start_end_times_df = temp_df.groupby(temp_df.columns.tolist(), as_index=False).size()
    return start_end_times_df, start_end_set_list

# def start_end_times_of_rules(dictionary):
#     list = []
#     for events in dictionary.values():
#         for event in events:
#             event_start_times = []
#             event_end_times = []
#             for appliance in event:
#                 event_start_times.append(appliance[0])
#                 event_end_times.append(appliance[1])
#             start_time = int(min(event_start_times).split(":")[0].split(" ")[1])
#             end_time = int(max(event_end_times).split(":")[0].split(" ")[1])
#             list.append([start_time] + [end_time])
#     temp_df = pd.DataFrame(list, columns = ['start_time', 'end_time'])
#     start_end_times_df = temp_df.groupby(temp_df.columns.tolist(),as_index=False).size()
#     return start_end_times_df


def SE_time_df(dataframe, TAT=0.1):
    """

    :param dataframe:
    :type dataframe:
    :param TAT: Time Associtaion Threshold
    :type TAT: fraction
    :return:
    :rtype:
    """
    rule_dict = {}
    max_day = -1
    for index, row in dataframe.iterrows():
        max_day = max(max_day, max(int(key) + 1 for key in row["time"].keys()))
    for index, row in dataframe.iterrows():
        df = pd.DataFrame({'TotalAbsSupport': [0 for _ in range(24)], 'AbsSupport': [0 for _ in range(24)]})
        start_end_df, day_hours_list = start_end_times_of_rules(row["time"])
        for end_index, start_end in start_end_df.iterrows():
            for hour in range(start_end['start_time'], start_end['end_time'] + 1):
                df['TotalAbsSupport'][hour] = df['TotalAbsSupport'][hour] + start_end['size']
        for day_hours in day_hours_list:
            df.loc[day_hours, 'AbsSupport'] = df.loc[day_hours, 'AbsSupport'] + 1
        df['EventCount'] = sum([len(events) for events in row['time'].values()])
        df['ExternalUtility'] = row['supp']
        df['RelSupport'] = df['AbsSupport'] / max_day
        df['TimeAssociation'] = np.where(df['AbsSupport'] / df['AbsSupport'].max() > TAT, 1, 0) #df['TotalAbsSupport'] / df['EventCount']
        rule_dict[row['pattern']] = df.copy()
    return rule_dict


def redundancy_filter_tool(dataframe, regex_str='MB[\w]*>[\w]*MB', multi_floor=True, rule_type='psn_app'):
    return dataframe.loc[(dataframe['pattern'].str.findall(regex_str).map(len) > 0) &
                         (dataframe['multi_floor'] == multi_floor) &
                         (dataframe['rule'] == rule_type)]


