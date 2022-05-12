from typing import Dict, Any, Tuple, Union, List

import numpy as np
import pandas as pd
from pandas import DataFrame

from Project.Database import Db
from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import find_average_power_consumption
from Project._05InferKnowledgeOfRules.infer_rules_functions import SE_time_df, json_to_dataframe


def extract_temporal_constraints(df, app) -> list:
    return [pd.DataFrame({app: df['TimeAssociation']}), pd.DataFrame({app: df[['Timespan', 'Flexibility']].max(0)})]


def load_app_stats(loaded_stats: dict) -> dict:
    app_stats = {}
    for key, value in loaded_stats.items():
        app_stats.update({key: extract_temporal_constraints(df=value, app=key)})
    return app_stats


def power_consumption(movable_appliances: list) -> pd.Series:
    return find_average_power_consumption()[movable_appliances]


def place_hours(duration: dict, use_order: list) -> dict:
    return_dict = {}
    total_duration = sum([duration[app] for app in use_order])

    if len(use_order) > 1:
        if total_duration == int(total_duration):
            use_order_lists = [use_order]
        else:
            use_order_lists = [use_order, use_order[::-1]]

        for uo in use_order_lists:
            prev_use_list = None
            for number, app in enumerate(uo):
                if number == 0:
                    use_list = list(1 for _ in range(int(duration[app])))
                    if duration[app] != int(duration[app]):
                        use_list.append(round(duration[app] % int(duration[app]), 4))

                else:
                    dur = duration[app]
                    if prev_use_list[-1] != 1:
                        use_list = list(0 for _ in range(len(prev_use_list) - 1))
                        use_list.append(round(min(dur, 1 - prev_use_list[-1]), 4))
                        dur = round(max(dur - use_list[-1], 0), 4)

                    else:
                        use_list = list(0 for _ in range(len(prev_use_list)))

                    if dur > 1:
                        use_list = use_list + list(1 for _ in range(int(dur)))
                        dur -= int(dur)

                    if dur > 0:
                        use_list.append(round(dur, 4))

                key = 'normal' if uo[-1] == use_order[-1] else 'reverse'
                return_dict[key] = return_dict.get(key, {})

                return_dict[key].update({
                    app: use_list
                })
                prev_use_list = use_list

        prev_use_list = []
        for number, app in enumerate(use_order):
            return_dict['normal'][app] = return_dict['normal'][app] + [0 for _ in
                                                                       range(24 - len(return_dict['normal'][app]))]

            if 'reverse' in return_dict.keys():
                if number == 0:
                    return_dict['reverse'][app] = [val for val in return_dict['reverse'][app][::-1] if val != 0]
                    prev_use_list = return_dict['reverse'][app]

                else:
                    if prev_use_list[-1] == 1:
                        use_list = [0 for _ in range(len(prev_use_list))]

                    else:
                        use_list = [0 for _ in range(len(prev_use_list) - 1)]

                    return_dict['reverse'][app] = use_list + [val for val in return_dict['reverse'][app][::-1] if
                                                              val != 0]
                    prev_use_list = return_dict['reverse'][app]

                return_dict['reverse'][app] = return_dict['reverse'][app] + [0 for _ in range(
                    24 - len(return_dict['reverse'][app]))]
    else:
        app = use_order[0]
        use_list = list(1 for _ in range(int(duration[app])))
        if duration[app] != int(duration[app]):
            use_list.append(round(duration[app] % int(duration[app]), 4))
            return_dict['reverse'] = {
                app: use_list[::-1] + [0 for _ in range(24 - len(use_list))]
            }

        return_dict['normal'] = {
            app: use_list + [0 for _ in range(24 - len(use_list))]
        }

    return_dict.update({
        'normal': pd.DataFrame(return_dict['normal'])
    })

    if 'reverse' in return_dict.keys():
        return_dict.update({
            'reverse': pd.DataFrame(return_dict['reverse'])
        })

    return return_dict


def appliance_use_matrix(time_association_df: pd.DataFrame, timespan: pd.Series, power_use: pd.Series,
                         use_order: list) -> tuple[pd.DataFrame, dict[pd.DataFrame, pd.DataFrame]]:
    power_use.sort_values(ascending=False,
                          inplace=True)
    time_association = time_association_df.max(1)
    timespan = timespan / 60

    energy_use_dict = {}
    hour_slots_dict = {}

    use_matrix_dict = place_hours(duration=timespan,
                                  use_order=use_order)

    for key, df in use_matrix_dict.items():
        energy_use_dict[key] = df.multiply(power_use[use_order]).div(1000).sum(1).to_list()
        hour_slots_dict[key] = df[lambda self: self.sum(1) > 0].shape[0]

    energy_matrix_dict = {}
    app_use_matrix_dict = {}
    start = 0
    for key, use_list in energy_use_dict.items():
        temp_energy_dict = {}
        for n in range(24 + 1 - hour_slots_dict[key]):
            rotate_list = [use_list[(i - n) % len(use_list)] for i, x in enumerate(use_list)]
            temp_energy_dict.update({(n + start): rotate_list})
        energy_matrix_dict[key] = pd.DataFrame(temp_energy_dict).fillna(0)
        for app in use_order:
            temp_use_dict = {}
            for n in range(24 + 1 - hour_slots_dict[key]):
                rotate_list = [use_matrix_dict[key][app].tolist()[(i - n) % len(use_list)] for i, x in
                               enumerate(use_list)]
                temp_use_dict.update({(n + start): rotate_list})
            app_use_matrix_dict[app] = app_use_matrix_dict.get(app, {})
            app_use_matrix_dict[app][key] = pd.DataFrame(temp_use_dict).fillna(0)
        start += energy_matrix_dict[key].shape[1]

    app_status_dict = {}
    if 'reverse' in energy_matrix_dict.keys():
        energy_matrix = pd.concat(objs=list(energy_matrix_dict.values()), axis=1)
        for app in use_order:
            app_status_dict[app] = pd.concat(objs=list(app_use_matrix_dict[app].values()), axis=1)
    else:
        energy_matrix = energy_matrix_dict['normal']
        for app in use_order:
            app_status_dict[app] = app_use_matrix_dict[app]

    energy_matrix = energy_matrix.loc[:, (lambda self: (self.sum(0).round(4) == self.T.dot(
        time_association).round(4)))]  # Selects only the hours that is time associated

    for app in use_order:
        app_status_dict[app] = app_status_dict[app][energy_matrix.columns.tolist()]

    return energy_matrix, app_status_dict


def slice_emission_vector(production_vectors: pd.DataFrame, day_number: int = 0) -> pd.Series:
    return production_vectors.loc[production_vectors.index.get_level_values(level=0).unique().tolist()[day_number]]


def find_min_hour(energy_matrix: pd.DataFrame, app_status_dict, emission_vector, day_number: int,
                  number_of_events: int):
    sliced_emission_vector = slice_emission_vector(emission_vector, day_number)

    return_df = pd.DataFrame()
    events = energy_matrix.T.dot(sliced_emission_vector)[lambda self: self == self.min()].head(1)
    if number_of_events == 1:
        index = events.index.tolist()[0]
        return_df[index] = energy_matrix[index].multiply(sliced_emission_vector)

        for app in app_status_dict:
            app_status_dict[app] = app_status_dict[app][events.index]

        return return_df, app_status_dict

    elif number_of_events > 1:
        condition = (lambda df:
                     (df.sum(0) != df.T.dot(
                         np.where(pd.DataFrame(df[events.index.tolist()]).max(1) > 0, 1, 0))))

        condition2 = (lambda df, events_index:
                      (df.loc[events_index] == 0).min(0))

        for _ in range(number_of_events - 1):
            events = pd.concat(objs=(
                events,
                energy_matrix.loc[:, (lambda self:
                                      condition(self) &
                                      condition2(self, events.index.tolist()))].T.dot(sliced_emission_vector)[
                    lambda self: self == self.min()].head(1)
            ))

        for index in events.index:
            return_df[index] = energy_matrix[index].multiply(sliced_emission_vector)

        for app in app_status_dict:
            app_status_dict[app] = app_status_dict[app][events.index.tolist()]

        return return_df, app_status_dict


def hourly_house_df(house_df: pd.DataFrame, aggregate_func: str) -> pd.DataFrame:
    house_df['Day'] = house_df['Timestamp'].dt.dayofyear
    house_df['Day'] = house_df['Day'] - house_df['Day'][0]
    # Find the first row index with negative value
    first_index = house_df.loc[lambda self: self['Day'] < 0].index.tolist()[0]
    house_df.loc[first_index:, 'Day'] = house_df.loc[first_index:, 'Day'] + 365

    house_df['Hour'] = house_df['Timestamp'].dt.hour
    house_df['Timestamp'] = pd.to_datetime(house_df['Timestamp'].dt.strftime('%Y-%m-%d %H'), format='%Y-%m-%d %H')
    if aggregate_func == 'max':
        house_df = house_df.groupby(['Day', 'Hour']).max()
    elif aggregate_func == 'sum':
        house_df = house_df.groupby(['Day', 'Hour']).sum()
    house_df.reset_index(inplace=True)
    house_df['Emission'] = 0
    return house_df


def optimise_house_df(house_df: pd.DataFrame, pattern_df: pd.DataFrame, emission_vector: pd.DataFrame,
                      movable_appliances: list, dependant_apps_rules: list) -> pd.DataFrame:
    pattern_app_stats = SE_time_df(dataframe=pattern_df,
                                   TAT=0.1,
                                   TS_quantile=0.9)
    all_app_stats = load_app_stats(loaded_stats=pattern_app_stats)
    power_consum = power_consumption(movable_appliances=movable_appliances)
    house_df[movable_appliances] = 0
    placed_apps = []
    for app in movable_appliances:
        if app not in placed_apps:
            pattern_events = pattern_df.copy().loc[lambda self: self['pattern'] == app, 'time'].values[0]
            in_rule = False
            for rule in dependant_apps_rules:
                if app in rule:
                    in_rule = True
                    if '->' in rule:
                        use_order = rule.split('->')
                        rule_app_stats = []
                        for rule_app in use_order:
                            rule_app_stats.append(all_app_stats[rule_app])
                        time_association_df = pd.concat(objs=[rule_app[0] for rule_app in rule_app_stats],
                                                        axis=1)
                        timespan = pd.concat(objs=[
                            rule_app[1][use_order[index]] for index, rule_app in enumerate(rule_app_stats)
                        ], axis=1).loc['Timespan']
                        power_use = power_consum[use_order]
                        break

            if not in_rule:
                app_stats = all_app_stats[app]
                time_association_df = app_stats[0]
                timespan = app_stats[1].loc['Timespan']
                power_use = power_consum[[app]]
                use_order = [app]

            energy_matrix, app_status_dict = appliance_use_matrix(time_association_df=time_association_df,
                                                                  timespan=timespan,
                                                                  power_use=power_use,
                                                                  use_order=use_order)

            for day in pattern_events.keys():
                event_count = len(pattern_events[day])
                events, statuses = find_min_hour(energy_matrix=energy_matrix.copy(),
                                                 app_status_dict=app_status_dict.copy(),
                                                 emission_vector=emission_vector,
                                                 day_number=int(day),
                                                 number_of_events=event_count)

                for column in events.columns:
                    for index, emission in events.loc[lambda self: self[column] > 0, column].iteritems():
                        house_df.loc[(lambda self: (self['Day'] == int(day)) &
                                                   (self['Hour'] == index)), 'Emission'] += emission
                        for appliance in statuses.keys():
                            app_status = pd.Series(dtype='float')
                            for col in statuses[appliance].columns:
                                app_status = pd.concat(objs=(app_status, statuses[appliance][col]))
                            house_df.loc[(lambda self: (self['Day'] == int(day))), appliance] = app_status

            placed_apps += use_order

    return house_df


def NZERTF_optimiser(year: int = 2) -> pd.DataFrame:
    movable_appliances = ['Load_StatusClothesWasher',
                          'Load_StatusDryerPowerTotal',
                          'Load_StatusApplianceDishwasher',
                          'Load_StatusPlugLoadVacuum',
                          'Load_StatusPlugLoadIron']

    NZERTF_house = Db.load_data(year=year,
                                hourly=False)[['Timestamp'] + movable_appliances]

    NZERTF_house = hourly_house_df(house_df=NZERTF_house,
                                   aggregate_func='max')

    production = Db.load_data(consumption=False,
                              production=True,
                              year=year)

    production = production.groupby([production.index.strftime('%Y-%m-%d'),
                                     production.index.hour]).max()['CO2(Grams)/kWh']

    pattern_df = json_to_dataframe(year=year,
                                   level=1,
                                   exclude_follows=True,
                                   with_redundancy=False)

    pattern_df = pattern_df.loc[lambda self: self['pattern'].isin(movable_appliances)]

    optimised_NZERTF_house = optimise_house_df(house_df=NZERTF_house.copy(),
                                               pattern_df=pattern_df,
                                               emission_vector=production,
                                               movable_appliances=movable_appliances,
                                               dependant_apps_rules=[
                                                   'Load_StatusClothesWasher->Load_StatusDryerPowerTotal'])

    optimised_NZERTF_house['CumulativeEmission'] = optimised_NZERTF_house['Emission'].cumsum()

    return optimised_NZERTF_house


if __name__ == '__main__':
    # rangee = [1 for _ in range(6)] + [0 for _ in range(12)] + [1 for _ in range(6)]
    # appliance_use_matrix(time_association_df=pd.DataFrame({'app1': rangee, 'app2': rangee, 'app3': rangee}),
    #                            timespan=pd.Series({'app1': 135, 'app2': 79, 'app3': 60}),
    #                            power_use=pd.Series({'app1': 121, 'app2': 1205, 'app3': 3308}),
    #                            use_order=['app1', 'app2', 'app3'])
    # print(place_hours(duration={'app1': 3.33, 'app2': 1.45, 'app3': 1},
    #                   use_order=['app1', 'app2', 'app3']))
    NZERTF_optimised = NZERTF_optimiser()
    print(NZERTF_optimised)
