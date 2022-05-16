import pandas as pd

from Project.Database import Db
from Project._05InferKnowledgeOfRules.infer_rules_functions import json_to_dataframe, SE_time_df
from Project._07OptimiseConsumption.optimisation_problem import load_app_stats, hourly_house_df, power_consumption, \
    optimise_house_df


def find_emissions(df, emission_vec):
    energy_vec = df.copy()['Consumption'].reset_index(drop=True)
    emission_vector = energy_vec.multiply(emission_vec)
    for index, emission in emission_vector[lambda self: self > 0].iteritems():
        df.loc[lambda self: (self['Hour'] == index), 'Emission'] += emission
    return df['Emission']


def emission_reduction(year: int = 2):
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
            1_000).T.multiply(
            production.reset_index(drop=True)).T
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

    NZERTF_emission['m.a.o'] = NZERTF_optimisation['m.a.o']['Emission'].sum()
    NZERTF_emission['m.a.u.o'] = NZERTF_optimisation['m.a.u.o']['Emission'].sum()

    return NZERTF_optimisation, NZERTF_emission


if __name__ == '__main__':
    optimisation, emission = emission_reduction(year=2)
    wr = optimisation['w.r']
    wor = optimisation['w.o.r']
    mao = optimisation['m.a.o']
    mauo = optimisation['m.a.u.o']
