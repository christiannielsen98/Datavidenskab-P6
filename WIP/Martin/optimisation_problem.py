import numpy as np
import pandas as pd

from Project.Database import Db
from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import find_average_power_consumption_per_minute
from Project._05InferKnowledgeOfRules.infer_rules_functions import SE_time_df, json_to_dataframe


def extract_temporal_constraints(df, app) -> list:
    return [pd.DataFrame({app: df['TimeAssociation']}), pd.DataFrame({app: df[['Timespan', 'Flexibility']].max(0)})]


def load_app_stats(loaded_stats: dict) -> dict:
    app_stats = {}
    for key, value in loaded_stats.items():
        app_stats.update({key: extract_temporal_constraints(df=value, app=key)})
    return app_stats


def power_consumption(movable_appliances: list) -> pd.Series:
    return find_average_power_consumption_per_minute()[movable_appliances]


def place_hours(remaining, start_row=0, first=True, exp_vector=None) -> pd.Series:
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


def place_expensive_appliance(remaining, hour_range) -> pd.Series:
    exp_use_vector = pd.Series(dtype='float64', index=range(24)).fillna(0)
    for hour in hour_range:
        exp_use_vector[hour] = min(remaining, 1)
        remaining = max(remaining - 1, 0)
        if remaining == 0:
            break
    return exp_use_vector


def place_cheap_appliance(remaining, hour_range, exp_vector) -> pd.Series:
    cheap_use_vector = pd.Series(dtype='float64', index=range(24)).fillna(0)
    for hour in hour_range:
        cheap_use_vector[hour] = min(remaining, 1) - exp_vector[hour]
        remaining = max(remaining - (1 - exp_vector[hour]), 0)
        if remaining == 0:
            break
    return cheap_use_vector


def appliance_use_matrix(time_association_df: pd.DataFrame, timespan: pd.Series, power_use: pd.Series,
                         use_order: list) -> pd.DataFrame:
    power_use.sort_values(ascending=False,
                          inplace=True)
    time_association = time_association_df.max(1)
    timespan = timespan / 60
    use_matrix = pd.DataFrame(columns=use_order,
                              index=range(24)).fillna(0)
    if len(power_use.index) > 1:
        exp_app = power_use.index[0]
        cheap_app = power_use.index[1]
        if use_order.index(exp_app) == 0:
            use_matrix[exp_app] = place_hours(remaining=timespan[exp_app])
            use_matrix[cheap_app] = place_hours(remaining=timespan[cheap_app],
                                                first=False,
                                                exp_vector=use_matrix[exp_app])
        else:
            start_row = int(np.ceil(timespan[lambda self: self.index[:use_order.index(exp_app)]].sum()))
            use_matrix[exp_app] = place_hours(remaining=timespan[exp_app],
                                              start_row=start_row,
                                              first=False)
            use_matrix[cheap_app] = place_hours(remaining=timespan[cheap_app],
                                                exp_vector=use_matrix[exp_app])

        use_matrix[exp_app] = use_matrix[exp_app] * power_use[exp_app] / 1000
        use_matrix[cheap_app] = use_matrix[cheap_app] * power_use[cheap_app] / 1000
        use_vec = pd.concat(objs=(use_matrix[exp_app],
                                  use_matrix[cheap_app]),
                            axis=1).sum(1)
        use_list = use_vec.tolist()
        hour_slots = use_vec[lambda self: self > 0].shape[0]

    else:
        use_matrix[use_order[0]] = place_hours(remaining=timespan[use_order[0]])
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


def slice_emission_vector(production_vectors: pd.DataFrame, day_number: int = 0) -> pd.Series:
    return production_vectors.loc[production_vectors.index.get_level_values(level=0).unique().tolist()[day_number]]


def find_min_hour(energy_matrix: pd.DataFrame, emission_vector, day_number: int, number_of_events: int):
    sliced_emission_vector = slice_emission_vector(emission_vector, day_number)

    return_df = pd.DataFrame()
    if number_of_events == 1:
        event = energy_matrix.T.dot(sliced_emission_vector)[lambda self: self == self.min()].head(1)
        index = event.index.tolist()[0]
        return_df[index] = energy_matrix[index].multiply(sliced_emission_vector)
        return return_df
    elif number_of_events > 1:
        events = energy_matrix.T.dot(sliced_emission_vector)[lambda self: self == self.min()].head(1)

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
                 condition2(self, events.index.tolist()))].T.dot(sliced_emission_vector)[
                    lambda self: self == self.min()].head(1)
            ))

        for index in events.index:
            return_df[index] = energy_matrix[index].multiply(sliced_emission_vector)
        return return_df


def find_event_count(pattern_df: pd.DataFrame, day_number: str = '0') -> int:
    return len(pattern_df['time'][day_number])


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
                                   LS_quantile=0.9)
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

            energy_matrix = appliance_use_matrix(time_association_df=time_association_df,
                                                 timespan=timespan,
                                                 power_use=power_use,
                                                 use_order=use_order)

            for day in pattern_events.keys():
                event_count = len(pattern_events[day])

                if event_count > 0:
                    events = find_min_hour(energy_matrix=energy_matrix,
                                           emission_vector=emission_vector,
                                           day_number=int(day),
                                           number_of_events=event_count)
                    for column in events.columns:
                        for index, emission in events.loc[lambda self: self[column] > 0, column].iteritems():
                            house_df.loc[(lambda self: (self['Day'] == int(day)) &
                                                       (self['Hour'] == index)), 'Emission'] += emission
                            house_df.loc[(lambda self: (self['Day'] == int(day)) &
                                                       (self['Hour'] == index)), app] = 1
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
    print(NZERTF_optimiser())
