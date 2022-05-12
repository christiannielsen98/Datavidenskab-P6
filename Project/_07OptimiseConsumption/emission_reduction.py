import pandas as pd

from Project.Database import Db
from optimisation_problem import hourly_house_df, slice_emission_vector, power_consumption_vector, NZERTF_optimiser


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

    power_consumption = power_consumption_vector(movable_appliances=movable_appliances)

    # with redundancy <- w.r
    # without redundancy <- w.o.r
    # movable appliances unoptimised <- m.a.u.o
    # movable appliances optimised <- m.a.o
    NZERTF_optimisation = {
        'w.r': Db.load_data(year=year, hourly=False),
        'w.o.r': Db.load_data(year=year, hourly=False, with_redundancy=False)
    }

    NZERTF_optimisation.update({
        'm.a.u.o': NZERTF_optimisation['w.o.r'].copy()[['Timestamp'] + movable_appliances]
    })

    timestamps = pd.to_datetime(
        NZERTF_optimisation['w.r']['Timestamp'].dt.strftime('%Y-%m-%d %H').unique(),
        format='%Y-%m-%d %H')

    production = Db.load_data(
        consumption=False,
        production=True,
        year=year)['CO2(Grams)/kWh'][lambda self: self.index.isin(timestamps)]
    production = production.groupby(pd.to_datetime(production.index.strftime('%Y-%m-%d %H'))).sum()

    for key, value in NZERTF_optimisation.items():
        df = value.copy()
        if all([att in df.columns for att in consumers]):
            df = hourly_house_df(house_df=df.copy(), aggregate_func='mean')
            df['Consumption'] = df[consumers].div(1_000).sum(1)
            df.drop(labels=consumers, inplace=True, axis=1)
        else:
            df = hourly_house_df(house_df=df, aggregate_func='mean')

            df['Consumption'] = df[movable_appliances].dot(power_consumption).div(1_000)

        for day in df['Day'].unique():
            df.loc[(lambda self: (self['Day'] == day)),
                   'Emission'] = find_emissions(df=df.copy().loc[lambda self: self['Day'] == day],
                                                emission_vec=slice_emission_vector(
                                                    production_vectors=production,
                                                    day_number=day))
        NZERTF_optimisation.update({
            key: df
        })

    NZERTF_optimisation.update({
        'm.a.o': NZERTF_optimiser()
    })

    NZERTF_emission = {}

    for key in NZERTF_optimisation.keys():
        NZERTF_emission.update({
            key: NZERTF_optimisation[key]['Emission'].sum()
        })
    return NZERTF_optimisation, NZERTF_emission
