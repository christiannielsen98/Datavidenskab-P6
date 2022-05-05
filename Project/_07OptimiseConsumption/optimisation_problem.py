import numpy as np
import pandas as pd

from Project.Database import Db
from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import find_average_power_consumption_per_minute
from Project._05InferKnowledgeOfRules.infer_rules_functions import SE_time_df, json_to_dataframe

movable_appliances = ['Load_StatusApplianceDishwasher', 'Load_StatusPlugLoadVacuum', 'Load_StatusClothesWasher',
                      'Load_StatusDryerPowerTotal', 'Load_StatusPlugLoadIron']


def extract_temporal_constraints(df, app):
    return [pd.DataFrame({app: df['TimeAssociation']}), pd.DataFrame({app: df[['Lifespan', 'Flexibility']].max(0)})]


def load_app_stats(loaded_stats: dict):
    app_stats = {}
    for key, value in loaded_stats.items():
        if key in movable_appliances:
            app_stats.update({key: extract_temporal_constraints(value, key)})
    return app_stats


def power_consumption():
    return find_average_power_consumption_per_minute()[movable_appliances]


def place_hours(remaining, start_row=0, first=True, exp_vector=None):
    app_life_ceiled = int(np.ceil(remaining))

    if exp_vector is None:
        hour_range = list(range(start_row,
                                start_row + app_life_ceiled))
        if not first:
            hour_range = hour_range[::-1]
        use_vector = place_expensive_appliance(remaining, hour_range)

    else:
        if first:
            end = exp_vector[lambda self: self > 0].index[0]
            hour_range = list(range(0, end + 1))
            hour_range = hour_range[::-1]
        else:
            start = exp_vector[lambda self: self > 0].index[-1]
            hour_range = list(range(start, 24))

        use_vector = place_cheap_appliance(remaining, hour_range, exp_vector)

    return use_vector


def place_expensive_appliance(remaining, hour_range):
    exp_use_vector = pd.Series(dtype='float64', index=range(24)).fillna(0)
    for hour in hour_range:
        exp_use_vector[hour] = min(remaining, 1)
        remaining = max(remaining - 1, 0)
        if remaining == 0:
            break
    return exp_use_vector


def place_cheap_appliance(remaining, hour_range, exp_vector):
    cheap_use_vector = pd.Series(dtype='float64', index=range(24)).fillna(0)
    for hour in hour_range:
        cheap_use_vector[hour] = min(remaining, 1) - exp_vector[hour]
        remaining = max(remaining - (1 - exp_vector[hour]), 0)
        if remaining == 0:
            break
    return cheap_use_vector


def appliance_use_matrix(time_association_df: pd.DataFrame, lifespan: pd.Series, power_use: pd.Series, use_order: list):
    power_use.sort_values(ascending=False,
                          inplace=True)
    time_association = time_association_df.max(1)
    lifespan = lifespan / 60
    use_matrix = pd.DataFrame(columns=use_order,
                              index=range(24)).fillna(0)
    if len(power_use.index) > 1:
        exp_app = power_use.index[0]
        cheap_app = power_use.index[1]
        if use_order.index(exp_app) == 0:
            use_matrix[exp_app] = place_hours(remaining=lifespan[exp_app])
            use_matrix[cheap_app] = place_hours(remaining=lifespan[cheap_app],
                                                first=False,
                                                exp_vector=use_matrix[exp_app])
        else:
            start_row = int(np.ceil(lifespan[lambda self: self.index[:use_order.index(exp_app)]].sum()))
            use_matrix[exp_app] = place_hours(remaining=lifespan[exp_app],
                                              start_row=start_row,
                                              first=False)
            use_matrix[cheap_app] = place_hours(remaining=lifespan[cheap_app],
                                                exp_vector=use_matrix[exp_app])

        use_matrix[exp_app] = use_matrix[exp_app] * power_use[exp_app] / 1000
        use_matrix[cheap_app] = use_matrix[cheap_app] * power_use[cheap_app] / 1000
        use_vec = pd.concat(objs=(use_matrix[exp_app],
                                  use_matrix[cheap_app]),
                            axis=1).sum(1)
        use_list = use_vec.tolist()
        hour_slots = use_vec[lambda self: self > 0].shape[0]

    else:
        use_matrix[use_order[0]] = place_hours(remaining=lifespan[use_order[0]][0])
        use_matrix[use_order[0]] = use_matrix[use_order[0]] * power_use[use_order[0]] / 1000
        use_list = use_matrix[use_order[0]].tolist()
        hour_slots = use_matrix[use_order[0]][lambda self: self > 0].shape[0]

    temp_dict = {}
    for n in range(24 + 1 - hour_slots):
        rotate_list = [use_list[(i - n) % len(use_list)] for i, x in enumerate(use_list)]
        temp_dict.update({n: rotate_list})

    energy_matrix = pd.DataFrame(temp_dict).fillna(0)
    energy_matrix = energy_matrix.loc[:, lambda self: self.sum(0) == self.T.dot(
        time_association)]  # Selects only the hours that is time associated
    return energy_matrix


def production_vector(production_vectors: pd.DataFrame, day_number: int = 0):
    return production_vectors.loc[production_vectors.index.get_level_values(level=0).unique().tolist()[day_number]]


def find_min_hour(energy_matrix: pd.DataFrame, day_number: int, number_of_events: int):
    production = Db.load_data(consumption=False, production=True, year=2)
    production_vectors = production.groupby([production.index.strftime('%Y-%m-%d'), production.index.hour]).sum()[
        'CO2(Grams)/kWh']
    emission_vector = production_vector(production_vectors, day_number)

    return_df = pd.DataFrame()
    if number_of_events == 1:
        event = energy_matrix.T.dot(emission_vector)[lambda self: self == self.min()].head(1)
        index = event.index.tolist()[0]
        return_df[index] = energy_matrix[index].multiply(emission_vector)
        return return_df
    elif number_of_events > 1:
        events = energy_matrix.T.dot(emission_vector)[lambda self: self == self.min()].head(1)

        condition = (lambda df:
                     (df.sum(0) != df.T.dot(np.where(pd.DataFrame(df[events.index.tolist()]).max(1) > 0, 1, 0))))
        condition2 = (lambda df, events_index:
                      (df.loc[events_index] == 0).min(0))

        for _ in range(number_of_events - 1):
            events = pd.concat(objs=(
                events,
                energy_matrix.loc[:,
                (lambda self:
                 condition(self) &
                 condition2(self, events.index.tolist()))].T.dot(emission_vector)[lambda self: self == self.min()].head(
                    1)
            )
            )

        for index in events.index:
            return_df[index] = energy_matrix[index].multiply(emission_vector)
        return return_df


def find_event_count(pattern_df: pd.DataFrame, appliance: str, day_number: int = 0):
    level1_movable_appliances = pattern_df.loc[lambda self: self['pattern'].isin(movable_appliances)].set_index(
        'pattern')
    try:
        return len(level1_movable_appliances.loc[appliance]['time'][f'{day_number}'])
    except:
        return 0


def hourly_house_df(house_df: pd.DataFrame, aggregate_func: str):
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


year2 = Db.load_data(year=2, hourly=False)[['Timestamp'] + movable_appliances]

year2 = hourly_house_df(year2, aggregate_func='max')


def optimise_house_df(house_df: pd.DataFrame):
    pattern_df = json_to_dataframe(year=2, level=1, exclude_follows=True, with_redundancy=False)
    pattern_app_stats = SE_time_df(pattern_df)
    all_app_stats = load_app_stats(pattern_app_stats)
    power_consum = power_consumption()
    house_df = year2.copy()
    house_df[movable_appliances] = 0
    for app in movable_appliances:
        app_stats = all_app_stats[app]
        energy_matrix = appliance_use_matrix(app_stats[0], app_stats[1][app], power_consum[[app]], [app])
        for day in house_df['Day'].unique():
            event_count = find_event_count(appliance=app, day_number=day)
            if event_count > 0:
                events = find_min_hour(energy_matrix=energy_matrix, day_number=day, number_of_events=event_count)
                for column in events.columns:
                    for index, emission in events.loc[lambda self: self[column] > 0, column].iteritems():
                        house_df.loc[
                            lambda self: (self['Day'] == day) & (self['Hour'] == index), 'Emission'] += emission
                        house_df.loc[lambda self: (self['Day'] == day) & (self['Hour'] == index), app] = 1
    return house_df


# level1_json_df = json_to_dataframe(year=2, level=1, exclude_follows=True, with_redundancy=False)
# level1_app_stats = SE_time_df(level1_json_df)
# temp_var = load_app_stats(level1_app_stats)
# clothes_washer = temp_var['Load_StatusClothesWasher']
# dryer = temp_var['Load_StatusDryerPowerTotal']
# association = pd.concat(objs=(dryer[0], clothes_washer[0]),
#                         axis=1)
# life = pd.concat(objs=(dryer[1]['Load_StatusDryerPowerTotal'],
#                        clothes_washer[1]['Load_StatusClothesWasher']),
#                  axis=1).loc['Lifespan']
# power = energy_consumption()[['Load_StatusDryerPowerTotal', 'Load_StatusClothesWasher']]
# print(appliance_use_matrix(association, life, power, ['Load_StatusDryerPowerTotal', 'Load_StatusClothesWasher']))
# print(find_min_hour(energy_matrix=appliance_use_matrix(association, life, power,
#                                                        ['Load_StatusClothesWasher', 'Load_StatusDryerPowerTotal']),
#                     day_number=5, number_of_events=2))
print(optimise_house_df(year2))