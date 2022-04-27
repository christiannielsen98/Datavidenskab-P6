import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from Project._01PreprocessData.data_cleaning import data_cleaner
from Project.Database import Db


def find_status_one(dataframe, app_status_atts):
    """
    Searches the dataframe for rows where all app_status_atts = 1
    :param dataframe: NZERTF DataFrame
    :param app_status_atts: Appliances to look for
    :return: dataframe rows
    """
    if isinstance(app_status_atts, list) and len(app_status_atts) >= 1:
        return dataframe.loc[(lambda self: self[app_status_atts].sum(1) == len(app_status_atts))]
    elif isinstance(app_status_atts, str):
        return dataframe.loc[(lambda self: self[app_status_atts] == 1)]
    else:
        return dataframe


def find_status_zero(dataframe, app_status_atts):
    """
    Searches the dataframe for rows where all app_status_atts = 0
    :param dataframe: NZERTF DataFrame
    :param app_status_atts: Appliances to look for
    :return: dataframe rows
    """
    if isinstance(app_status_atts, list) and len(app_status_atts) >= 1:
        return dataframe.loc[(lambda self: self[app_status_atts].sum(1) == 0)]
    elif isinstance(app_status_atts, str):
        return dataframe.loc[(lambda self: self[app_status_atts] == 0)]
    else:
        return dataframe


def find_status_ppl_same_floor(meta_data_status_rows, status_row):
    """
    Searches NZERTF meta data for person status attributes simulating person on same floor as appliance
    :param meta_data_status_rows: All meta data rows of appliance status attributes (columns in NZERTF Data)
    :param status_row: Single meta data appliance status row
    :return: A list of person status attributes
    """
    return meta_data_status_rows.loc[(lambda self: (self.index.str.contains('SensHeat')) & (
            self['Measurement_Floor'] == status_row['Measurement_Floor']))].index.tolist()


def find_status_siblings(meta_data_status_rows, status_row, app_status_att):
    """
    Searches the meta data for appliances with a mutual consumption attribute (sibling) with app_status_att attribute
    :param meta_data_status_rows: All meta data rows of appliance status attributes (columns in NZERTF Data)
    :param status_row: Single meta data appliance status row
    :param app_status_att: Appliance to compare consumer with
    :return: A list of appliance status attributes
    """
    return meta_data_status_rows.loc[(lambda self: (~self.index.isin([app_status_att])) & (
            self['Consumer_Match'] == status_row['Consumer_Match']))].index.tolist()


def find_status_siblings_zero(dataframe, meta_data_status_rows, status_row, app_status_att):
    """
    Searches the dataframe for rows where siblings are turned off
    :param dataframe: NZERTF DataFrame
    :param meta_data_status_rows: All meta data rows of appliance status attributes (columns in NZERTF Data)
    :param status_row: Single meta data appliance status row
    :param app_status_att: Appliance to find siblings of
    :return: Dataframe rows where siblings are turned off
    """
    return find_status_zero(dataframe=dataframe,
                            app_status_atts=find_status_siblings(meta_data_status_rows=meta_data_status_rows,
                                                                 status_row=status_row, app_status_att=app_status_att))


def remove_zero_consumption(dataframe, status_row):
    """
    Removes all rows where the consumer attribute = 0
    :param dataframe: NZERTF DataFrame
    :param status_row: Single meta data appliance status row
    :return: Dataframe rows where consumer != 0
    """
    return dataframe.loc[lambda self: self[status_row['Consumer_Match']] > 0]


def create_redundancy_dataframes():
    """
    Creates a binary redundancy dataframe for each year of the NZERTF data indexed by timestamp. 1 flags redundancy
    :return: The redundancy dataframe
    """
    redundancy_df_dict = {}
    for year in [1, 2]:
        NZERTF, NZERTF_meta = Db.load_data(hourly=False, meta=True, year=year)
        meta_data_status_rows = NZERTF_meta.loc[(lambda self: self['Units'] == 'BinaryStatus')]
        redundancy_df = pd.DataFrame(NZERTF['Timestamp'])

        appliance_status = meta_data_status_rows.loc[
            (lambda self:
             (~self.index.str.contains('SensHeat')) &
             (~self.index.isin(
                 ['Load_StatusApplianceCooktop', 'Load_StatusApplianceDishwasher', 'Load_StatusApplianceOven',
                  'Load_StatusApplianceRangeHood', 'Load_StatusLatentload', 'Load_StatusPlugLoadCoffeeMaker',
                  'Load_StatusPlugLoadSlowCooker', 'Load_StatusPlugLoadToaster', 'Load_StatusPlugLoadToasterOven',
                  'Load_StatusClothesWasher', 'Load_StatusDryerPowerTotal', 'Load_StatusRefrigerator',
                  'Load_StatusHeatPumpWaterHeater'])) &
             (self['Subsystem'] == 'Loads'))]

        for app_status_att, row in appliance_status.iterrows():
            tmp_df = find_status_one(NZERTF, app_status_att)

            if app_status_att == 'Load_StatusPlugLoadVacuum':
                ppl = meta_data_status_rows.loc[(lambda self: (self.index.str.contains('SensHeat')))].index.tolist()

            elif row['Measurement_Location'] == 'Bedroom2':
                ppl = 'Load_StatusSensHeatChildAUP'

            elif row['Measurement_Location'] == 'Bedroom3':
                ppl = 'Load_StatusSensHeatChildBUP'

            elif row['Measurement_Location'] in ['MasterBedroom', 'MasterBathroom']:
                ppl = ['Load_StatusSensHeatPrntAUP', 'Load_StatusSensHeatPrntBUP']

            elif row['Measurement_Location'] == 'Bedroom4':
                ppl = ['Load_StatusSensHeatPrntADOWN', 'Load_StatusSensHeatPrntBDOWN']

            else:
                ppl = find_status_ppl_same_floor(meta_data_status_rows=meta_data_status_rows, status_row=row)

            tmp_df = find_status_zero(tmp_df, ppl)

            redundancy_df.loc[tmp_df['Timestamp'].index.tolist(), app_status_att] = 1

        redundancy_df.fillna(0, inplace=True)
        redundancy_df_dict[f'Year{year}'] = redundancy_df.copy()

    return redundancy_df_dict


def find_average_power_consumption():
    """
    Estimates average power consumption per minute of NZERTF appliances into a dataframe
    :return: A dataframe of mean power consumption of NZERTF appliances
    """
    NZERTF = pd.DataFrame()

    for year in [1, 2]:
        tmp_NZERTF, NZERTF_meta = Db.load_data(meta=True, hourly=False, year=year)
        # tmp_NZERTF, NZERTF_meta = data_cleaner(hourly=False, year=year)
        NZERTF = pd.concat(objs=(NZERTF, tmp_NZERTF), axis=0)

    consumption_series = pd.Series(dtype='float32')
    meta_data_status_rows = NZERTF_meta.loc[
        lambda self: (self['Units'] == 'BinaryStatus') & (~self['Consumer_Match'].isna())]

    for consumer in meta_data_status_rows['Consumer_Match'].unique():
        meta_rows = meta_data_status_rows.loc[lambda self: self['Consumer_Match'] == consumer]
        df_copy = NZERTF[meta_rows.index.tolist() + meta_rows['Consumer_Match'].unique().tolist()].copy()
        df_copy.dropna(inplace=True)
        estimator = LinearRegression(fit_intercept=False, positive=True)

        estimator.fit(X=df_copy[meta_rows.index.tolist()], y=df_copy[meta_rows['Consumer_Match'][0]])
        consumption_series = pd.concat(objs=(
            consumption_series,
            pd.Series(dict(zip(meta_rows.index.tolist(), estimator.coef_)))
        ))

    return pd.DataFrame(consumption_series, columns=['PowerConsumption']).round()


if __name__ == '__main__':
    print(find_average_power_consumption())
