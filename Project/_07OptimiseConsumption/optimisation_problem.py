import numpy as np
import pandas as pd

from Project.Database import Db
from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import find_average_power_consumption_per_minute
from Project._05InferKnowledgeOfRules.infer_rules_functions import SE_time_df, json_to_dataframe

movable_appliances = ['Load_StatusApplianceDishwasher', 'Load_StatusPlugLoadVacuum', 'Load_StatusClothesWasher',
                      'Load_StatusDryerPowerTotal', 'Load_StatusPlugLoadIron']


def extract_temporal_constraints(df, app):
    return [pd.DataFrame({app: df['TimeAssociation']}), pd.DataFrame({app: df[['Lifespan', 'Flexibility']].max(0)})]


def load_app_stats(year: int = 1):
    loaded_stats = SE_time_df(json_to_dataframe(year=year, level=1, with_redundancy=False))
    app_stats = {}
    for key, value in loaded_stats.items():
        if key in movable_appliances:
            app_stats.update({key: extract_temporal_constraints(value, key)})
    return app_stats


def energy_consumption():
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

    return energy_matrix


temp_var = load_app_stats()
clothes_washer = temp_var['Load_StatusClothesWasher']
dryer = temp_var['Load_StatusDryerPowerTotal']
association = pd.concat(objs=(dryer[0], clothes_washer[0]),
                        axis=1)
life = pd.concat(objs=(dryer[1]['Load_StatusDryerPowerTotal'],
                       clothes_washer[1]['Load_StatusClothesWasher']),
                 axis=1).loc['Lifespan']
power = energy_consumption()[['Load_StatusDryerPowerTotal', 'Load_StatusClothesWasher']]
print(appliance_use_matrix(association, life, power, ['Load_StatusDryerPowerTotal', 'Load_StatusClothesWasher']))

# temp_var = load_app_stats()
# dish_washer = temp_var['Load_StatusApplianceDishwasher']
# association = dish_washer[0]
# life = dish_washer[1][['Load_StatusApplianceDishwasher']]
# power = energy_consumption()[['Load_StatusApplianceDishwasher']]
# print(appliance_use_matrix(association, life, power, ['Load_StatusApplianceDishwasher']))
