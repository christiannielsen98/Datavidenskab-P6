import json
import re

import numpy as np
import pandas as pd

from Project.Database import Db
from Project._04TPMAlgorithm.transform_for_TPM_algorithm import light_location_dict


def json_to_dataframe(year, level, exclude_follows=True, with_redundancy=True):
    redundancy = '' if with_redundancy else '_no_redundancy'
    json_file = json.load(
        open(Db.get_save_file_directory(
            f"output/NZERTF_year{year}{redundancy}_minsup0.14_minconf_0.5/level{level}.json")))
    if "," in json_file[0]["name_node"]:
        level_df = pd.DataFrame(columns=["pattern", "supp", "conf", "time"])
        level1 = False
        for i in json_file:
            for j in i["patterns"]:
                level_df.loc[level_df.shape[0]] = j
    else:
        level_df = pd.DataFrame(columns=["name_node", "supp", "conf", "time"])
        for i in json_file:
            level_df.loc[level_df.shape[0]] = i
        level_df.rename(columns={'name_node': 'pattern'}, inplace=True)
        level1 = True

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


def start_end_times_of_rules(dictionary, LS_quantile):
    """
    It takes a dictionary of events and returns a dataframe of the start and end hours of each event, a
    list of the start and end hours of each event, and the lifespan of each event
    
    :param dictionary: the dictionary of events
    :param LS_quantile: the quantile of the lifespan of the events that you want to use as the lifespan
    of the events
    :return: start_end_hours_df is a dataframe with the start and end hours of each event.
    start_end_set_list is a list of lists of the start and end hours of each event.
    lifespan is the lifespan of each event.
    """
    start_end_list = []
    start_end_set_list = []
    lifespan = pd.Series(dtype='int32')
    for day in dictionary.values():
        start_end_set = set()
        for event in day:
            try:
                event_start_times = []
                event_end_times = []
                for appliance in event:
                    event_start_times.append(appliance[0])
                    event_end_times.append(appliance[1])
                start_hour = int(min(event_start_times).split(":")[0].split(" ")[1])
                end_hour = int(max(event_end_times).split(":")[0].split(" ")[1])
                start_end_list.append([start_hour] + [end_hour])
                start_end_set.update({hour for hour in range(start_hour, end_hour + 1)})
                lifespan = pd.concat(
                    objs=(lifespan,
                          pd.Series((pd.to_datetime(event_end_times, format='%Y-%m-%d %H:%M:%S') -
                                     pd.to_datetime(event_start_times,
                                                    format='%Y-%m-%d %H:%M:%S')).total_seconds() / 60)))
            except:
                start_hour = int(event[0].split(":")[0].split(" ")[1])
                end_hour = int(event[1].split(":")[0].split(" ")[1])
                start_end_list.append([start_hour] + [end_hour])
                start_end_set.update({hour for hour in range(start_hour, end_hour + 1)})
                lifespan = pd.concat(
                    objs=(lifespan,
                          pd.Series((pd.to_datetime(event[1], format='%Y-%m-%d %H:%M:%S') -
                                     pd.to_datetime(event[0], format='%Y-%m-%d %H:%M:%S')).total_seconds() / 60)))
        start_end_set_list.append(list(start_end_set))

    temp_df = pd.DataFrame(start_end_list, columns=['start_hour', 'end_hour'])
    start_end_hours_df = temp_df.groupby(temp_df.columns.tolist(), as_index=False).size()
    lifespan = int(round(lifespan.quantile(LS_quantile), 0))
    return start_end_hours_df, start_end_set_list, lifespan


def SE_time_df(dataframe, TAT=0.1, LS_quantile=0.9):
    """
    This function takes a dataframe of rules and returns a dataframe of rules that match the regex_str,
    multi_floor, and rule_type parameters
    
    :param dataframe: the dataframe that contains the rules
    :param TAT: Time Association Threshold
    :type TAT: fraction
    :param LS_quantile: The quantile of the lifespan of the rule that you want to consider
    :return: A dictionary of dataframes with the following columns:
        - TotalAbsSupport
        - AbsSupport
        - EventCount
        - ExternalUtility
        - RelSupport
        - TimeAssociation 
        - Flexibility
        - Lifespan
    """
    rule_dict = {}
    max_day = -1
    for index, row in dataframe.iterrows():
        max_day = max(max_day, max(int(key) + 1 for key in row["time"].keys()))
    for index, row in dataframe.iterrows():
        df = pd.DataFrame({'TotalAbsSupport': [0 for _ in range(24)], 'AbsSupport': [0 for _ in range(24)]})
        start_end_df, day_hours_list, lifespan = start_end_times_of_rules(row["time"], LS_quantile)
        for end_index, start_end in start_end_df.iterrows():
            for hour in range(start_end['start_hour'], start_end['end_hour'] + 1):
                df['TotalAbsSupport'][hour] = df['TotalAbsSupport'][hour] + start_end['size']
        for day_hours in day_hours_list:
            df.loc[day_hours, 'AbsSupport'] = df.loc[day_hours, 'AbsSupport'] + 1
        df['EventCount'] = sum([len(events) for events in row['time'].values()])
        df['ExternalUtility'] = row['supp']
        df['RelSupport'] = df['AbsSupport'] / max_day
        df['TimeAssociation'] = np.where(df['AbsSupport'] / df['AbsSupport'].max() > TAT, 1,
                                         0)  # df['TotalAbsSupport'] / df['EventCount']
        df['Flexibility'] = df['TimeAssociation'].mean()
        df['Lifespan'] = lifespan
        rule_dict[row['pattern']] = df.copy()
    return rule_dict


def redundancy_filter_tool(dataframe, regex_str='MB[\w]*>[\w]*MB', multi_floor=True, rule_type='psn_app'):
    return dataframe.loc[(dataframe['pattern'].str.findall(regex_str).map(len) > 0) &
                         (dataframe['multi_floor'] == multi_floor) &
                         (dataframe['rule'] == rule_type)]
