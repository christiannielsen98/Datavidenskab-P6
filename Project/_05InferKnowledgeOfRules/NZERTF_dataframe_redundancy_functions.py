import pandas as pd

from Project.Database import Db


def find_status_one(dataframe, status_atts):
    if isinstance(status_atts, list) and len(status_atts) >= 1:
        return dataframe.loc[(lambda self: self[status_atts].sum(1) == len(status_atts))]
    elif isinstance(status_atts, str):
        return dataframe.loc[(lambda self: self[status_atts] == 1)]
    else:
        return dataframe


def find_status_zero(dataframe, status_atts):
    if isinstance(status_atts, list) and len(status_atts) >= 1:
        return dataframe.loc[(lambda self: self[status_atts].sum(1) == 0)]
    elif isinstance(status_atts, str):
        return dataframe.loc[(lambda self: self[status_atts] == 0)]
    else:
        return dataframe


def find_status_ppl_same_floor(status_cols, status_row):
    tmp_meta = status_cols.copy()
    return tmp_meta.loc[(lambda self: (self.index.str.contains('SensHeat')) & (
            self['Measurement_Floor'] == status_row['Measurement_Floor']))].index.tolist()


def find_status_siblings(status_cols, status_row, status_att):
    tmp_meta = status_cols.copy()
    tmp_meta.drop(status_att, inplace=True)
    return tmp_meta.loc[(lambda self: (self['Load_Match'] == status_row['Load_Match']))].index.tolist()


def find_status_siblings_zero(dataframe, status_cols, status_row, status_att):
    return find_status_zero(dataframe, find_status_siblings(status_cols, status_row, status_att))


def remove_zero_consumption(dataframe, status_row):
    return dataframe.loc[lambda self: self[status_row['Load_Match']] > 0]


def find_power_consumption(dataframe, status_cols, status_row, status_att):
    dataframe = remove_zero_consumption(find_status_one(dataframe, status_att), status_row)
    # return dataframe[status_row['Load_Match']].div(dataframe[find_status_siblings(status_row, status_att)].sum(1) + 1).mean()
    dataframe_siblings_zero = find_status_siblings_zero(dataframe, status_cols, status_row, status_att)
    if dataframe_siblings_zero.shape[0] > 0:
        return dataframe_siblings_zero[status_row['Load_Match']].mean()
    else:
        return dataframe[status_row['Load_Match']].div(
            dataframe[find_status_siblings(status_cols, status_row, status_att)].sum(1) + 1).mean()


def find_redundancy(house, status_cols):
    redundancy_df = pd.DataFrame(columns=['RedundantMinutes', 'PowerConsumption', 'EnergyConsumed'])
    timestamp_df = pd.DataFrame(house['Timestamp'])

    appliance_status = status_cols.loc[(lambda self:
                                        (~self.index.str.contains('SensHeat')) &
                                        (~self.index.isin(
                                            ['Load_StatusApplianceCooktop', 'Load_StatusApplianceDishwasher',
                                             'Load_StatusApplianceOven', 'Load_StatusApplianceRangeHood',
                                             'Load_StatusLatentload', 'Load_StatusPlugLoadCoffeeMaker',
                                             'Load_StatusPlugLoadSlowCooker', 'Load_StatusPlugLoadToaster',
                                             'Load_StatusPlugLoadToasterOven', 'Load_StatusClothesWasher',
                                             'Load_StatusDryerPowerTotal'])) &
                                        (self['Subsystem'] == 'Loads'))]

    for att, row in appliance_status.iterrows():

        power_consumption = find_power_consumption(house, status_cols, row, att)
        tmp_df = find_status_one(house, att)

        if att == 'Load_StatusPlugLoadVacuum':
            ppl = status_cols.loc[(lambda self: (self.index.str.contains('SensHeat')))].index.tolist()

        elif row['Measurement_Location'] == 'Bedroom2':
            ppl = 'Load_StatusSensHeatChildAUP'

        elif row['Measurement_Location'] == 'Bedroom3':
            ppl = 'Load_StatusSensHeatChildBUP'

        elif row['Measurement_Location'] in ['MasterBedroom', 'MasterBathroom']:
            ppl = ['Load_StatusSensHeatPrntAUP', 'Load_StatusSensHeatPrntBUP']

        elif row['Measurement_Location'] == 'Bedroom4':
            ppl = ['Load_StatusSensHeatPrntADOWN', 'Load_StatusSensHeatPrntBDOWN']

        else:
            ppl = find_status_ppl_same_floor(status_cols, row)

        tmp_df = find_status_zero(tmp_df, ppl)

        redundancy_df.loc[att] = {
            'PowerConsumption': power_consumption,
            'RedundantMinutes': tmp_df.shape[0],
            'EnergyConsumed': f'{tmp_df.shape[0] * power_consumption / 60 / 1000} kWh'
        }
        timestamp_df.loc[tmp_df['Timestamp'].index.tolist(), att] = 1

    timestamp_df.fillna(0, inplace=True)
    return redundancy_df, timestamp_df


def create_redundancy_dataframes():
    year_redundancy_df_dict = {}
    timestamp_dict = {}

    for year in [1, 2]:
        NZERTF, NZERTF_meta = Db.load_data(hourly=False, meta=True, year=year)
        status_cols = NZERTF_meta.loc[(lambda self: self['Units'] == 'BinaryStatus')]

        year_redundancy_df_dict[f'Year{year}'], timestamp_dict[f'Year{year}'] = find_redundancy(NZERTF, status_cols)

    for year in [1, 2]:
        year_redundancy_df_dict[f'Year{year}']['PowerConsumption'] = pd.DataFrame(
            year_redundancy_df_dict[f'Year{year}']['PowerConsumption']).merge(
            right=year_redundancy_df_dict[f'Year{2 if year == 1 else 1}']['PowerConsumption'], how='left',
            left_index=True, right_index=True).mean(1)
        year_redundancy_df_dict[f'Year{year}']['EnergyConsumed'] = year_redundancy_df_dict[f"Year{year}"][
            "RedundantMinutes"].multiply(year_redundancy_df_dict[f"Year{year}"]["PowerConsumption"]).div(60000)

    return year_redundancy_df_dict, timestamp_dict
