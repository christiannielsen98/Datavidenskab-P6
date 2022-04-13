import pandas as pd
import plotly.express as px

from Project.Database import Db

NZERTF_year1, NZERTF_year1_meta, NZERTF_year1_production = Db.load_data(hourly=False, meta=True, production=True)

consumers = NZERTF_year1_meta.loc[lambda self: (~self['Consumer_Match'].isna()), 'Consumer_Match'].tolist() + \
            NZERTF_year1_meta.loc[
                lambda self: (self['Units'] == 'W') & (self.index.str.contains('HVAC_'))].index.tolist()

producers = NZERTF_year1_meta.loc[lambda self: (self['Units'] == 'W') & (self.index.str.contains('PV_'))].index.tolist()

NZERTF_year1[producers] = NZERTF_year1[producers] * -1

NZERTF_year1 = NZERTF_year1[consumers + producers].set_index(
    pd.to_datetime(NZERTF_year1['Timestamp'], format='%Y-%m-%d %H:%M:%S'))

NZERTF_year1_energy = NZERTF_year1.div(60).div(1000)

del NZERTF_year1, NZERTF_year1_meta

NZERTF_year1_energy['EnergyBalance(kWh)'] = NZERTF_year1_energy.sum(1)
NZERTF_year1_energy['GridConsumption(kWh)'] = NZERTF_year1_energy['EnergyBalance(kWh)'].map(lambda val: max(val, 0))

NZERTF_year1_energy = NZERTF_year1_energy.groupby(NZERTF_year1_energy.index.strftime('%Y-%m-%d %H')).sum()

NZERTF_year1_energy.set_index(
    pd.to_datetime(pd.to_datetime(NZERTF_year1_energy.index, format='%Y-%m-%d %H').strftime('%Y-%m-%d %H:%M:%S'),
                   format='%Y-%m-%d %H:%M:%S'), inplace=True)

NZERTF_year1_energy['CO2Emission(Grams)'] = NZERTF_year1_energy['GridConsumption(kWh)'].multiply(
    NZERTF_year1_production.loc[
        NZERTF_year1_production.index.isin(
            NZERTF_year1_energy[
                'GridConsumption(kWh)'].index), 'CO2(Grams)/kWh'])

NZERTF_year1_energy['CO2Emission(kg)'] = NZERTF_year1_energy['CO2Emission(Grams)'].div(1000)

print(NZERTF_year1_energy)
print('Totals:')
print(f'  Energy balance           : {round(NZERTF_year1_energy["EnergyBalance(kWh)"].sum(), 2)}kWh')
print(f'  Energy consumed from grid: {round(NZERTF_year1_energy["GridConsumption(kWh)"].sum(), 2)}kWh')
print(f'  CO2 Emission             : {round(NZERTF_year1_energy["CO2Emission(kg)"].sum(), 2)}kg')

px.line(NZERTF_year1_energy, y=['CO2Emission(kg)', 'GridConsumption(kWh)', 'EnergyBalance(kWh)'],
        x=NZERTF_year1_energy.index).show()
