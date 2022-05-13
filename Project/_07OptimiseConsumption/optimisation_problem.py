import pandas as pd

from Project.Database import Db
from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import find_average_power_consumption
from Project._05InferKnowledgeOfRules.infer_rules_functions import SE_time_df, json_to_dataframe


def extract_temporal_constraints(df, app) -> list:
    return [pd.DataFrame({app: df['TimeAssociation']}),
            pd.DataFrame({app: df[['TimespanMean', 'TimespanMin', 'TimespanMax', 'Flexibility']].max(0)})]


def load_app_stats(loaded_stats: dict) -> dict:
    app_stats = {}
    for key, value in loaded_stats.items():
        app_stats.update({key: extract_temporal_constraints(df=value, app=key)})
    return app_stats


def power_consumption(movable_appliances: list) -> pd.Series:
    return find_average_power_consumption()[movable_appliances]


def place_hours(duration: pd.Series, use_order: list) -> dict:
    return_dict = {}
    total_max_duration = sum([duration[app]['TimespanMax'] for app in use_order])

    if len(use_order) > 1:
        if total_max_duration == int(total_max_duration):
            use_order_lists = [use_order]
        else:
            use_order_lists = [use_order, use_order[::-1]]

        for uo in use_order_lists:
            prev_use_list = None
            for number, app in enumerate(uo):
                if number == 0:
                    dur = duration[app]['TimespanMax']
                    use_list = list(1 for _ in range(int(dur)))
                    if dur != int(dur):
                        use_list.append(round(dur - int(dur), 4))

                else:
                    dur = duration[app]['TimespanMax']
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
                    app: use_list.copy()
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
        dur = duration[app]['TimespanMax']
        use_list = list(1 for _ in range(int(dur)))
        if dur != int(dur):
            use_list.append(round(dur - int(dur), 4))
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
    timespan = timespan.div(60)

    energy_factor = timespan.loc['TimespanMean'].div(timespan.loc['TimespanMax'])

    energy_use_dict = {}
    hour_slots_dict = {}

    use_matrix_dict = place_hours(duration=timespan,
                                  use_order=use_order)

    for key, df in use_matrix_dict.items():
        energy_use_dict[key] = df.multiply(energy_factor).multiply(power_use[use_order]).sum(1).to_list()
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
        start += 24

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
        app_status_dict[app] = app_status_dict[app].loc[:, (lambda self: (self.columns.isin(energy_matrix.columns)))]

    return energy_matrix.div(1_000), app_status_dict


def slice_emission_vector(production_vectors: pd.DataFrame, day_number: int = 0) -> pd.Series:
    return production_vectors.loc[production_vectors.index.get_level_values(level=0).unique().tolist()[day_number]]


def find_min_hour(energy_matrix: pd.DataFrame, app_status_dict, emission_vector, day_number: int,
                  number_of_events: int):
    sliced_emission_vector = slice_emission_vector(emission_vector, day_number)

    emissions = pd.DataFrame()
    events = energy_matrix.T.dot(sliced_emission_vector)[lambda self: self == self.min()].head(1)

    if number_of_events > 1:
        condition = (lambda df:
                     (df.T.dot(df[events.index.tolist()].max(1)) == 0))

        condition2 = (lambda df:
                      (df.loc[(events.index % 24).tolist()] == 0).min(0))

        condition3 = (lambda app_status:
                      app_status.sum(0) == app_status.T.dot(pd.concat(objs=(app_status.max(1),
                                                                            app_status[events.index.tolist()].max(1)),
                                                                      axis=1).sum(1) <= 1))

        for _ in range(number_of_events - 1):
            events = pd.concat(objs=(events,
                                     energy_matrix.loc[:,
                                     condition3(app_status_dict[list(app_status_dict.keys())[0]])
                                     # (condition3(app_status_dict[key]) for key in app_status_dict.keys()).all()
                                     # (lambda self:
                                     # & condition2(self)
                                     # )
                                     ].T.dot(sliced_emission_vector)[
                                         lambda self: self == self.min()].head(1)))

        for index in events.index:
            emissions[index] = energy_matrix[index].multiply(sliced_emission_vector)

        for app in app_status_dict.keys():
            app_status_dict[app] = app_status_dict[app][emissions.columns.tolist()].sum(1)

        emissions = emissions.sum(1)

    elif number_of_events == 1:
        index = events.index.tolist()[0]
        emissions[index] = energy_matrix[index].multiply(sliced_emission_vector)

        for app in app_status_dict:
            app_status_dict[app] = app_status_dict[app][emissions.columns.tolist()[0]]

        emissions = emissions[index]

    return emissions, app_status_dict


def hourly_house_df(house_df: pd.DataFrame, aggregate_func: str) -> pd.DataFrame:
    house_df['Timestamp'] = pd.to_datetime(house_df['Timestamp'].dt.strftime('%Y-%m-%d %H'), format='%Y-%m-%d %H')
    house_df = house_df.groupby('Timestamp')
    timestamps = pd.date_range(start=house_df.max().index.min(), end=house_df.max().index.max(), freq='H')
    new_house_df = pd.DataFrame(index=timestamps)

    new_house_df['Day'] = new_house_df.index.dayofyear
    new_house_df['Day'] = new_house_df['Day'] - new_house_df['Day'][0]
    # Find the first row index with negative value
    first_index = new_house_df.loc[lambda self: self['Day'] < 0].index.tolist()[0]
    new_house_df.loc[first_index:, 'Day'] = new_house_df.loc[first_index:, 'Day'] + 365

    new_house_df['Hour'] = new_house_df.index.hour

    if aggregate_func == 'max':
        new_house_df = pd.concat(objs=(new_house_df, house_df.max()), axis=1)
    elif aggregate_func == 'min':
        new_house_df = pd.concat(objs=(new_house_df, house_df.min()), axis=1)
    elif aggregate_func == 'sum':
        new_house_df = pd.concat(objs=(new_house_df, house_df.sum()), axis=1)
    elif aggregate_func == 'mean':
        new_house_df = pd.concat(objs=(new_house_df, house_df.mean()), axis=1)

    new_house_df.reset_index(inplace=True)
    new_house_df.rename(columns={'index': 'Timestamp'}, inplace=True)

    return new_house_df.fillna(0)


def find_event_count(day: list, durations: pd.Series):
    event_count = 0
    for event in day:
        event_start = pd.to_datetime(event[0], format='%Y-%m-%d %H:%M:%S')
        event_end = pd.to_datetime(event[1], format='%Y-%m-%d %H:%M:%S')
        if durations['TimespanMin'] <= (event_end - event_start).total_seconds() / 60 <= durations['TimespanMax']:
            event_count += 1

    return event_count


def optimise_house_df(house_df: pd.DataFrame, pattern_df: pd.DataFrame, emission_vector: pd.DataFrame,
                      movable_appliances: list, dependant_apps_rules: list,
                      power_consum: pd.Series = pd.Series(dtype='float')) -> pd.DataFrame:
    pattern_app_stats = SE_time_df(dataframe=pattern_df,
                                   TAT=0.1)

    all_app_stats = load_app_stats(loaded_stats=pattern_app_stats)

    if len(power_consum) == 0:
        power_consum = power_consumption(movable_appliances=movable_appliances)

    house_df = hourly_house_df(house_df=house_df, aggregate_func='mean')
    old_df = house_df.copy()[movable_appliances]
    house_df[movable_appliances] = 0
    house_df['Emission(g/Wh)'] = emission_vector.reset_index(drop=True)
    house_df['Emission'] = 0

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
                        ], axis=1).loc[['TimespanMin', 'TimespanMax', 'TimespanMean']]
                        power_use = power_consum[use_order]
                        durations = timespan[app]
                    break

            if not in_rule:
                app_stats = all_app_stats[app]
                time_association_df = app_stats[0]
                timespan = app_stats[1].loc[['TimespanMin', 'TimespanMax', 'TimespanMean']]
                durations = timespan[app]
                power_use = power_consum[[app]]
                use_order = [app]

            energy_matrix, app_status_dict = appliance_use_matrix(time_association_df=time_association_df,
                                                                  timespan=timespan,
                                                                  power_use=power_use,
                                                                  use_order=use_order)

            for day in pattern_events.keys():
                event_count = find_event_count(day=pattern_events[day], durations=durations)

                if event_count > 0:
                    emissions, statuses = find_min_hour(energy_matrix=energy_matrix.copy(),
                                                        app_status_dict=app_status_dict.copy(),
                                                        emission_vector=emission_vector,
                                                        day_number=int(day),
                                                        number_of_events=event_count)

                    for index, emission in emissions.loc[lambda self: self > 0].iteritems():
                        house_df.loc[(lambda self: (self['Day'] == int(day)) &
                                                   (self['Hour'] == index)), 'Emission'] += emission

                        for appliance in statuses.keys():
                            house_df.loc[(lambda self: (self['Day'] == int(day)) &
                                                       (self['Hour'] == index)), appliance] = statuses[appliance][index]

            placed_apps += use_order

    old_df['OldEmission'] = old_df.dot(power_consum).div(1000).multiply(house_df['Emission(g/Wh)'])
    old_df.rename(columns={col: 'Old' + col for col in movable_appliances}, inplace=True)
    house_df = pd.concat(objs=(house_df, old_df), axis=1)

    return house_df.fillna(0)


def NZERTF_optimiser(year: int = 2) -> pd.DataFrame:
    movable_appliances = ['Load_StatusClothesWasher',
                          'Load_StatusDryerPowerTotal',
                          'Load_StatusApplianceDishwasher',
                          'Load_StatusPlugLoadVacuum',
                          'Load_StatusPlugLoadIron']

    NZERTF_house = Db.load_data(year=year,
                                hourly=False)[['Timestamp'] + movable_appliances]

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

    optimised_NZERTF_house = optimise_house_df(house_df=NZERTF_house.copy(),
                                               pattern_df=pattern_df,
                                               emission_vector=production,
                                               movable_appliances=movable_appliances,
                                               dependant_apps_rules=[
                                                   'Load_StatusClothesWasher->Load_StatusDryerPowerTotal'],
                                               power_consum=power_consumption(movable_appliances=movable_appliances))

    optimised_NZERTF_house['CumulativeEmission'] = optimised_NZERTF_house['Emission'].cumsum()
    optimised_NZERTF_house['OldCumulativeEmission'] = optimised_NZERTF_house['OldEmission'].cumsum()

    return optimised_NZERTF_house


if __name__ == '__main__':
    NZERTF_optimised = NZERTF_optimiser(2)
    print(NZERTF_optimised)
