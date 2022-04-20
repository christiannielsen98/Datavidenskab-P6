import re

import pandas as pd
import numpy as np

from Project.Database import Db
from Project._01PreprocessData.data_matcher import find_status_consumer_match

room_floor_dict = {
    'Attic': 'Attic',
    'Basement': 'Basement',
    'Bath1': '1stFloor',
    'Bath2': '2ndFloor',
    'Bedroom2': '2ndFloor',
    'Bedroom3': '2ndFloor',
    'Bedroom4': '1stFloor',
    'Breezeway': '1stFloor',
    'DiningRoom': '1stFloor',
    'EntryHallway': '1stFloor',
    'Kitchen': '1stFloor',
    'LivingRoom': '1stFloor',
    'MasterBath': '2ndFloor',
    'MasterBedroom': '2ndFloor',
    'MBath': '2ndFloor',
    'MBedroom': '2ndFloor',
    'Mudroom': '1stFloor',
    'Outdoor': 'Outdoor',
    'Utility': '1stFloor'
}


def data_cleaner(hourly, year):
    house, meta = Db.load_data(hourly=hourly, meta=True, year=year, from_csv=True)

    meta.set_index('Unnamed: 0', inplace=True)
    # meta = meta.loc[~((~pd.isnull(house)).sum(0) <= (house.shape[0] / 2))]

    status_columns = meta.loc[(lambda self: self['Units'] == 'Binary Status')].index.tolist()
    consumer_columns = meta.loc[(lambda self: self['Units'] == 'W')].index.tolist()

    # house[(house[status_columns] != 0) & (house[status_columns] != 1)] = np.NaN
    # house = house.ffill()
    house[status_columns] = np.where(house[status_columns] > 0, 1, 0)

    # meta = meta.loc[(house != house.shift(1)).sum(0) > 1]
    house = house[['Timestamp'] + meta.index.tolist()]

    house['Timestamp'] = house['Timestamp'].str.split('-0[45]:00', expand=True)[0]
    house['Timestamp'] = pd.to_datetime(house['Timestamp'], format='%Y-%m-%d %H:%M:%S')

    house['Load_StatusRefrigerator'] = np.where(house['Elec_PowerRefrigerator'] > 0, 1, 0)
    house['Load_StatusMicrowave'] = np.where(house['Elec_PowerMicrowave'] > 0, 1, 0)
    house['Load_StatusClothesWasher'] = np.where(house['Elec_PowerClothesWasher'] > 0, 1, 0)
    house['Load_StatusDryerPowerTotal'] = np.where(house['Load_DryerPowerTotal'] > 0, 1, 0)
    house['Load_StatusHeatPumpWaterHeater'] = np.where(house['DHW_HeatPumpWaterHeaterPowerTotal'] > 6.92, 1, 0)

    meta.loc['Load_StatusRefrigerator'] = {
        'Subsystem': 'Loads',
        'Measurement_Location': 'Kitchen',
        'Parameter': 'Status_OnOff',
        'Description': 'Number to indicate whether the refrigerator is activated (1: Yes, O: No)',
        'Units': 'BinaryStatus',
        'Aggregation_Method': 'Average',
        'min_value': 0,
        'max_value': 1,
        'Measurement_Floor': 'Basement'
    }
    meta.loc['Load_StatusMicrowave'] = {
        'Subsystem': 'Loads',
        'Measurement_Location': 'Kitchen',
        'Parameter': 'Status_OnOff',
        'Description': 'Number to indicate whether the microwave oven is activated (1: Yes, O: No)',
        'Units': 'BinaryStatus',
        'Aggregation_Method': 'Average',
        'min_value': 0,
        'max_value': 1,
        'Measurement_Floor': 'Basement'
    }
    meta.loc['Load_StatusClothesWasher'] = {
        'Subsystem': 'Loads',
        'Measurement_Location': 'Utility',
        'Parameter': 'Status_OnOff',
        'Description': 'Number to indicate whether clothes washer is activated (1: Yes, O: No)',
        'Units': 'BinaryStatus',
        'Aggregation_Method': 'Average',
        'min_value': 0,
        'max_value': 1,
        'Measurement_Floor': '1stFloor'
    }
    meta.loc['Load_StatusDryerPowerTotal'] = {
        'Subsystem': 'Loads',
        'Measurement_Location': 'Utility',
        'Parameter': 'Status_OnOff',
        'Description': 'Number to indicate whether dryer is activated (1: Yes, O: No)',
        'Units': 'BinaryStatus',
        'Aggregation_Method': 'Average',
        'min_value': 0,
        'max_value': 1,
        'Measurement_Floor': '1stFloor'
    }
    meta.loc['Load_StatusHeatPumpWaterHeater'] = {
        'Subsystem': 'Loads',
        'Measurement_Location': 'Basement',
        'Parameter': 'Status_OnOff',
        'Description': 'Number to indicate whether heat pump water heater is activated (1: Yes, O: No)',
        'Units': 'BinaryStatus',
        'Aggregation_Method': 'Average',
        'min_value': 0,
        'max_value': 1,
        'Measurement_Floor': 'Basement'
    }
    meta['Standby_Power'] = house[consumer_columns].describe().loc['50%', (lambda self:
                                                                           (self.loc['25%'] == self.loc['50%']) |
                                                                           (self.loc['50%'] == self.loc['75%']))]
    meta.loc[consumer_columns, 'Standby_Power'] = meta.loc[consumer_columns, 'Standby_Power'].fillna(0)

    for col in ['Subsystem', 'Measurement_Location', 'Parameter', 'Units']:
        meta[col] = meta[col].str.replace(' ', '')

    meta['Measurement_Floor'] = [
        room_floor_dict[row['Measurement_Location']] if row['Measurement_Location'] in room_floor_dict.keys() else
        re.findall('[12][sn][td]', index)[0] + 'Floor' for index, row in meta.iterrows()]

    matches_dict = find_status_consumer_match(meta)

    for subsystem, status_dict in matches_dict.items():
        for status, consumer_dict in status_dict.items():
            if consumer_dict is not None:
                meta.loc[status, subsystem + '_Match'] = consumer_dict['consumer']
                meta.loc[consumer_dict['consumer'], 'Status_Match'] = status

    DHW_not_na = (lambda df: (~df['DHW_Match'].isna()) & (df['Consumer_Match'].isna()))
    load_not_na = (lambda df: (~df['Load_Match'].isna()) & (df['Consumer_Match'].isna()))
    elec_not_na = (lambda df: (~df['Elec_Match'].isna()) & (df['Consumer_Match'].isna()))

    meta['Consumer_Match'] = pd.NA
    meta.loc[lambda self: DHW_not_na(self), 'Consumer_Match'] = meta.loc[lambda self: DHW_not_na(self), 'DHW_Match']
    meta.loc[lambda self: elec_not_na(self), 'Consumer_Match'] = meta.loc[
        lambda self: elec_not_na(self), 'Elec_Match']
    meta.loc[lambda self: load_not_na(self), 'Consumer_Match'] = meta.loc[
        lambda self: load_not_na(self), 'Load_Match']
    meta.loc[lambda self: self['Load_Match'].str.contains('Standby|Total') & (
        ~self['Load_Match'].isna()), 'Consumer_Match'] = meta.loc[
        lambda self: self['Load_Match'].str.contains('Standby|Total') & (~self['Load_Match'].isna()), 'Load_Match']

    meta.loc['Load_StatusApplianceCooktop', 'Consumer_Match'] = 'Elec_PowerGarbageDisposal'
    meta.loc['Load_StatusEntryHallLights', 'Consumer_Match'] = 'Elec_PowerLights1stFloorB'
    meta.loc['Load_StatusBA1Lights', 'Consumer_Match'] = 'Elec_PowerLights1stFloorB'
    meta.loc[lambda self: self['Consumer_Match'] == 'Elec_PowerPlugsInstMBRA', 'Consumer_Match'] = 'Load_MBRPlugLoadsPowerUsage'
    meta.loc['Load_StatusPlugLoadHairDryerCurlIron', 'Consumer_Match'] = 'Elec_PowerPlugsMBAEast'

    meta.drop('DHW_Match', axis=1, inplace=True)

    return house, meta


if __name__ == '__main__':
    for hourly in [True, False]:
        for year in [1, 2]:
            time_base = 'hour' if hourly else 'minute'
            NZERTF, NZERTF_meta = data_cleaner(hourly, year)
            Db.pickle_dataframe(dataframe=NZERTF_meta, filename=f'Metadata-{time_base}-year{year}.pkl')
            Db.pickle_dataframe(dataframe=NZERTF, filename=f'All-Subsystems-{time_base}-year{year}.pkl')
