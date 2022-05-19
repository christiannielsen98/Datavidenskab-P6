import re

import numpy as np
import pandas as pd

from Project.Database import Db
from Project._05InferKnowledgeOfRules.infer_rules_functions import json_to_dataframe, SE_time_df
from Project._07OptimiseConsumption.optimisation_problem import load_app_stats, hourly_house_df, power_consumption, \
    optimise_house_df


def find_emissions(df, emission_vec):
    """
    For each hour in the dataframe, multiply the energy consumption by the emission factor and add the result to the
    emission column

    :param df: the dataframe that contains the energy consumption data
    :param emission_vec: a vector of emissions per unit of energy consumption
    :return: The emission column of the dataframe.
    """
    energy_vec = df.copy()['Consumption'].reset_index(drop=True)
    emission_vector = energy_vec.multiply(emission_vec)
    for index, emission in emission_vector[lambda self: self > 0].iteritems():
        df.loc[lambda self: (self['Hour'] == index), 'Emission'] += emission
    return df['Emission']


def emission_reduction(year: int = 2):
    """
    It takes the data from the database, and optimises the movable appliances to reduce the overall emission

    :param year: int = 2, defaults to 2
    :type year: int (optional)
    :return: A dictionary of dataframes and a dictionary of emissions.
    """
    meta = Db.load_data(meta=True, hourly=False, year=year, consumption=False).loc[
        lambda self: (~self['Consumer_Match'].isna()), 'Consumer_Match']

    movable_appliances = ['Load_StatusApplianceDishwasher', 'Load_StatusPlugLoadVacuum', 'Load_StatusClothesWasher',
                          'Load_StatusDryerPowerTotal', 'Load_StatusPlugLoadIron']

    consumers = meta.tolist()

    power_consumption_vector = power_consumption(movable_appliances=meta.index.tolist())

    # with redundancy <- w.r
    # without redundancy <- w.o.r
    # movable appliances optimisation <- m.a.o
    NZERTF_optimisation = {
        'w.r': Db.load_data(year=year, hourly=False),
        'w.o.r': Db.load_data(year=year, hourly=False, with_redundancy=False)
    }

    production = Db.load_data(consumption=False,
                              production=True,
                              year=year)

    production = production.groupby([production.index.strftime('%Y-%m-%d'),
                                     production.index.hour]).max()['CO2(g/Wh)']

    pattern_df = json_to_dataframe(year=year,
                                   level=1,
                                   exclude_follows=True,
                                   with_redundancy=False)

    pattern_df = pattern_df.loc[lambda self: self['pattern'].isin(movable_appliances)]

    power_factor = load_app_stats(SE_time_df(pattern_df))
    power_factor = pd.DataFrame(
        {col: df_list[1][df_list[1].columns[0]] for col, df_list in power_factor.items()},
        index=list(power_factor[list(power_factor.keys())[0]][1].index))
    time_span_min = power_factor.loc['TimespanMin']
    time_span_max = power_factor.loc['TimespanMax']
    time_span_mean = power_factor.loc['TimespanMean']
    power_factor = power_factor.loc['TimespanMean'].div(power_factor.loc['TimespanMax'])[movable_appliances]

    NZERTF_optimisation['m.a.o'] = optimise_house_df(
        house_df=NZERTF_optimisation['w.o.r'].copy()[['Timestamp'] + movable_appliances],
        pattern_df=pattern_df,
        emission_vector=production,
        movable_appliances=movable_appliances,
        dependant_apps_rules=[
            'Load_StatusClothesWasher->Load_StatusDryerPowerTotal'],
        power_consum=power_consumption_vector[movable_appliances])

    for key, value in NZERTF_optimisation.items():
        df = value.copy()
        if all([att in df.columns for att in consumers]):
            df = hourly_house_df(house_df=df.copy(), aggregate_func='mean')
            df['Consumption'] = df[consumers].sum(1)
            df['Emission'] = df['Consumption'].div(1_000).multiply(production.reset_index(drop=True))
            df.drop(labels=consumers, inplace=True, axis=1)

            NZERTF_optimisation.update({
                key: df
            })

    NZERTF_emission = {}

    for key in NZERTF_optimisation.keys():
        NZERTF_emission.update({
            key: NZERTF_optimisation[key]['Emission'].sum()
        })

    NZERTF_optimisation['w.r'][[f'{col}Emission' for col in meta.index.tolist()]] = \
        NZERTF_optimisation['w.r'][meta.index.tolist()].multiply(
            power_consumption_vector[meta.index.tolist()].T).div(
            1_000).T.multiply(production.reset_index(drop=True)).T
    NZERTF_optimisation['w.o.r'][[f'{col}Emission' for col in meta.index.tolist()]] = \
        NZERTF_optimisation['w.o.r'][meta.index.tolist()].multiply(
            power_consumption_vector[meta.index.tolist()].T).div(
            1_000).T.multiply(production.reset_index(drop=True)).T

    NZERTF_optimisation['m.a.u.o'] = NZERTF_optimisation['m.a.o'].copy()[['Timestamp', 'Day', 'Hour']]
    NZERTF_optimisation['m.a.u.o'][movable_appliances] = NZERTF_optimisation['m.a.o'].copy()[
        [f'Old{col}' for col in movable_appliances]]
    NZERTF_optimisation['m.a.u.o'][['Emission(g/Wh)', 'Emission']] = NZERTF_optimisation['m.a.o'].copy()[
        ['Emission(g/Wh)', 'OldEmission']]

    NZERTF_optimisation['m.a.o'][[f'{col}Emission' for col in movable_appliances]] = \
        NZERTF_optimisation['m.a.o'][movable_appliances].multiply(
            power_consumption_vector[movable_appliances].multiply(power_factor).T).div(
            1_000).T.multiply(production.reset_index(drop=True)).T
    NZERTF_optimisation['m.a.u.o'][[f'{col}Emission' for col in movable_appliances]] = \
        NZERTF_optimisation['m.a.u.o'][movable_appliances].multiply(
            power_consumption_vector[movable_appliances].T).div(
            1_000).T.multiply(production.reset_index(drop=True)).T

    NZERTF_optimisation['m.a.o'].drop([col for col in NZERTF_optimisation['m.a.o'].columns if 'Old' in col],
                                      axis=1,
                                      inplace=True)

    NZERTF_optimisation['m.a.o'][
        [f'{col}EventProportion' for col in movable_appliances]] = NZERTF_optimisation['m.a.o'][
        movable_appliances].div(time_span_max[movable_appliances].div(60).round(4)).where(
        NZERTF_optimisation['m.a.o'][movable_appliances] > 0).fillna(0)

    NZERTF_optimisation['m.a.u.o'][
        [f'{col}EventProportion' for col in movable_appliances]] = NZERTF_optimisation['m.a.u.o'][
        movable_appliances].div(time_span_mean[movable_appliances].div(60).round(4)).where(
        NZERTF_optimisation['m.a.u.o'][movable_appliances] > 0).fillna(0).round(1)

    NZERTF_emission['m.a.o'] = NZERTF_optimisation['m.a.o']['Emission'].sum()
    NZERTF_emission['m.a.u.o'] = NZERTF_optimisation['m.a.u.o']['Emission'].sum()

    return NZERTF_optimisation, NZERTF_emission


def create_intervals(start_, end_, start_index_, end_index_, start_frac_, end_frac_):
    """
    The function takes in the start and end times of a given interval, the start and end indices of the interval, and the
    start and end fractions of the interval. It then returns the start and end times of the interval

    :param start_: the start time of the interval
    :param end_: the end time of the interval
    :param start_index_: the index of the start time in the list of times
    :param end_index_: the index of the end of the interval
    :param start_frac_: The fraction of the hour that the start time is in
    :param end_frac_: the fraction of the last hour that the event ends in
    :return: the start and end times of the intervals.
    """
    if start_index_ != end_index_:
        diff_start_ = start_ + pd.DateOffset(hours=1, minutes=0) - start_
        minutes_start_ = diff_start_.seconds / 60 * start_frac_
        start_ = start_ + pd.DateOffset(hours=1, minutes=0) - pd.DateOffset(hours=0, minutes=minutes_start_)

        diff_end_ = end_ - (end_ - pd.DateOffset(hours=1, minutes=0))
        minutes_end_ = diff_end_.seconds / 60 * end_frac_
        end_ = end_ + pd.DateOffset(hours=0, minutes=minutes_end_)
    else:
        diff_start_ = start_ + pd.DateOffset(hours=1, minutes=0) - start_
        minutes_start_ = diff_start_.seconds / 60 * start_frac_
        start_ = start_ + pd.DateOffset(hours=1, minutes=0) - pd.DateOffset(hours=0, minutes=minutes_start_)

        end_ = end_ + pd.DateOffset(hours=1, minutes=0)

    return start_, end_


def create_gantt_data_and_event_dataframes(optim: dict):
    """
    It takes the optimised dataframes and extracts the appliance events from them

    :param year: int = 2, defaults to 2
    :type year: int (optional)
    :return: appliance_job_list, uo_event_df, o_event_df
    """

    NZERTF_meta = Db.load_data(hourly=False, meta=True, year=2, with_redundancy=True, consumption=False)
    appliance_job_list = []
    uo_event_df = pd.DataFrame()
    o_event_df = pd.DataFrame()
    for df in [optim['m.a.u.o'], optim['m.a.o']]:
        df['Day'] = df["Timestamp"].dt.dayofyear
        df['Day'] = df['Day'] - df['Day'][0]
        # Find the first row index with negative value
        first_index = df.loc[lambda self: self['Day'] < 0].index.tolist()[0]
        df.loc[first_index:, 'Day'] = df.loc[first_index:, 'Day'] + 365

        # Extract appliances from the meta data
        appliance_condition = [x for x in optim['m.a.o'].columns.tolist() if
                               "Status" in x and 'Emission' not in x and 'EventProportion' not in x]
        appliance_location = pd.DataFrame(
            NZERTF_meta.loc[appliance_condition, "Measurement_Location"].sort_values(ascending=True))
        # Combine minute data with the extracted appliances into a dataframe
        minute_appliances_status = df[["Timestamp"] + appliance_location.index.tolist()].copy()
        for appliance, appliance_row in appliance_location.iterrows():
            name = " ".join(re.findall('[A-Z][^A-Z]*', ''.join(
                ''.join(''.join(appliance.split('PowerTotal')).split('Appliance')).split('PlugLoad')).split(
                "Load_Status")[
                -1]))
            appliance_switch = minute_appliances_status[(np.where(
                minute_appliances_status[appliance] > 0, 1, 0) != np.where(
                minute_appliances_status[appliance].shift(1) > 0, 1, 0))][["Timestamp", appliance]][1:]
            appliance_switch.reset_index(inplace=True, drop=True)
            event_length = round(df.loc[lambda self: self[f'{appliance}EventProportion'] > 0, appliance].div(
                df.loc[lambda self: self[f'{appliance}EventProportion'] > 0,
                       f'{appliance}EventProportion']).mean(), 4)
            for index, row in appliance_switch.iterrows():
                if row[appliance]:
                    try:
                        start = row["Timestamp"]
                        start_index = df.loc[lambda self: self['Timestamp'] == start].index.tolist()[0]
                        end = appliance_switch.loc[index + 1, "Timestamp"] - pd.DateOffset(hours=1, minutes=0)
                        end_index = df.loc[lambda self: self['Timestamp'] == end].index.tolist()[0]

                        start_row_frac = 1
                        temp_start = row["Timestamp"]
                        temp_start_index = start_index
                        temp_start_frac = df.loc[lambda self: self['Timestamp'] == temp_start, appliance].values[0]

                        events_indexes = df.loc[lambda self: self['Timestamp'].between(temp_start, end)].index.tolist()
                        events_length = df.loc[events_indexes, appliance].sum()
                        event_proportion = df.loc[events_indexes, f'{appliance}EventProportion'].sum()
                        event_count = int(max(round(event_proportion, 0), 1))
                        event_length = events_length / event_count

                        for number in range(event_count):
                            temp_end_index = df.loc[start_index: end_index + 1].loc[
                                lambda self: self[appliance].cumsum().div(event_length).subtract(
                                    number) >= 1, appliance].index[0]
                            temp_end = df.loc[temp_end_index, 'Timestamp']
                            end_row_frac = df.loc[start_index: temp_end_index,
                                           appliance].cumsum().div(event_length).subtract(number).round(4).values[-1]
                            emission = df.loc[temp_start_index: temp_end_index, f'{appliance}Emission'].round(2)
                            emission[temp_start_index] = round(emission[temp_start_index] * start_row_frac, 2)

                            if end_row_frac > 1:
                                end_row_frac = round(2 - end_row_frac, 4)
                                temp_end_frac = df.loc[temp_end_index, appliance] * end_row_frac
                                emission[temp_end_index] = round(emission[temp_end_index] * end_row_frac, 2)
                                next_start_row_frac = round(1 - end_row_frac, 4)
                                next_start_index = temp_end_index
                                next_start_frac = round(df.loc[temp_end_index, appliance] * next_start_row_frac, 4)
                            else:
                                temp_end_frac = df.loc[temp_end_index, appliance]
                                next_start_row_frac = 1
                                next_start_index = temp_end_index + 1
                                next_start_frac = df.loc[temp_end_index + 1, appliance]

                            event_start, event_end = create_intervals(temp_start, temp_end, temp_start_index,
                                                                      temp_end_index, temp_start_frac, temp_end_frac)

                            if optim['m.a.u.o']['Emission'].sum() == df['Emission'].sum():
                                appliance_job_list.append({"appliance": name + ' before',
                                                           "start": event_start,
                                                           "end": event_end,
                                                           "location": appliance_row["Measurement_Location"],
                                                           'CO2 Emission (kg)': round(emission.sum(), 2)})
                                if 'Dryer' in name:
                                    uo_event_df = pd.concat(
                                        objs=(uo_event_df, pd.DataFrame({'Day': [df.loc[temp_start_index, 'Day']],
                                                                         'CO2 Emission (kg)': [round(
                                                                             emission.sum(), 2)]}))
                                        ).reset_index(drop=True)
                            else:
                                appliance_job_list.append({"appliance": name + ' after',
                                                           "start": event_start,
                                                           "end": event_end,
                                                           "location": appliance_row["Measurement_Location"],
                                                           'CO2 Emission (kg)': round(emission.sum(), 2)})
                                if 'Dryer' in name:
                                    o_event_df = pd.concat(
                                        objs=(o_event_df, pd.DataFrame({'Day': [df.loc[temp_start_index, 'Day']],
                                                                        'CO2 Emission (kg)': [round(
                                                                            emission.sum(), 2)]}))
                                        ).reset_index(drop=True)

                            start_row_frac = next_start_row_frac
                            temp_start_index = next_start_index
                            temp_start = df.loc[temp_start_index, 'Timestamp']
                            temp_start_frac = next_start_frac

                    except:
                        continue
    return appliance_job_list, uo_event_df, o_event_df


if __name__ == '__main__':
    optimisation, emission = emission_reduction(year=2)
    wr = optimisation['w.r']
    wor = optimisation['w.o.r']
    mao = optimisation['m.a.o']
    mauo = optimisation['m.a.u.o']
