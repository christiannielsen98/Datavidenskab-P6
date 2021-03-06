import pandas as pd

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
                  'Load_StatusMicrowave', 'Load_StatusApplianceRangeHood', 'Load_StatusLatentload',
                  'Load_StatusPlugLoadCoffeeMaker', 'Load_StatusPlugLoadSlowCooker', 'Load_StatusPlugLoadToaster',
                  'Load_StatusPlugLoadToasterOven', 'Load_StatusClothesWasher', 'Load_StatusDryerPowerTotal',
                  'Load_StatusRefrigerator', 'Load_StatusHeatPumpWaterHeater'])) &
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
    Estimates average power consumption of NZERTF appliances into a dataframe
    :return: A dataframe of mean power consumption of NZERTF appliances
    """
    consumption_dict = {}
    for year in [1, 2]:
        consumption_dict[f'year{year}PowerConsumption'] = consumption_dict.get(f'year{year}PowerConsumption',
                                                                               {})
        NZERTF, NZERTF_meta = Db.load_data(meta=True, hourly=False, year=year)
        meta_data_status_rows = NZERTF_meta.loc[
            lambda self: (self['Units'] == 'BinaryStatus') & (~self['Consumer_Match'].isna())]

        for app_status_att, status_row in meta_data_status_rows.iterrows():
            dataframe = remove_zero_consumption(find_status_one(NZERTF, app_status_att), status_row)
            dataframe_siblings_zero = find_status_siblings_zero(dataframe=dataframe,
                                                                meta_data_status_rows=meta_data_status_rows,
                                                                status_row=status_row, app_status_att=app_status_att)
            if dataframe_siblings_zero.shape[0] > 0:
                consumption_dict[f'year{year}PowerConsumption'].update({
                    app_status_att: dataframe_siblings_zero[status_row['Consumer_Match']].mean()
                })
            else:
                consumption_dict[f'year{year}PowerConsumption'].update({
                    app_status_att: dataframe[status_row['Consumer_Match']].div(dataframe[find_status_siblings(
                        meta_data_status_rows=meta_data_status_rows, status_row=status_row,
                        app_status_att=app_status_att)].sum(1) + 1).mean()
                })

    consumption_df = pd.DataFrame(consumption_dict)

    return consumption_df.mean(1).round()


def remove_redundant_power_consumption_from_data():
    power_consumption = find_average_power_consumption()
    NZERTF_redundancy = create_redundancy_dataframes()
    for year in [1, 2]:
        status_columns = NZERTF_redundancy[f'Year{year}'].columns[1:].tolist()

        NZERTF, NZERTF_meta = Db.load_data(hourly=False, meta=True, year=year)
        temp_df = NZERTF.copy()
        NZERTF[status_columns] = NZERTF.where(NZERTF_redundancy[f'Year{year}'][status_columns] == 0)[
            status_columns].fillna(0)

        for status_att, meta_row in NZERTF_meta.loc[status_columns].iterrows():
            for index, row in NZERTF.loc[NZERTF_redundancy[f'Year{year}'][status_att] == 1].iterrows():
                temp_var = NZERTF.loc[index, meta_row['Consumer_Match']] - power_consumption[status_att]
                temp_var = max(NZERTF_meta.loc[meta_row['Consumer_Match'], 'Standby_Power'], temp_var)
                NZERTF.loc[index, meta_row['Consumer_Match']] = temp_var

        consumers = Db.load_data(hourly=False, meta=True, consumption=False, year=year).loc[lambda self: (~self['Consumer_Match'].isna()), 'Consumer_Match'].tolist()
        temp_df['year'] = temp_df['Timestamp'].dt.year
        temp_df['month'] = temp_df['Timestamp'].dt.month
        temp_df['day'] = temp_df['Timestamp'].dt.day
        temp_df['hour'] = temp_df['Timestamp'].dt.hour
        temp_df = temp_df.groupby(['year', 'month', 'day', 'hour']).mean()
        temp_df['Consumption'] = temp_df[consumers].div(1000).sum(1)
        NZERTF['year'] = NZERTF['Timestamp'].dt.year
        NZERTF['month'] = NZERTF['Timestamp'].dt.month
        NZERTF['day'] = NZERTF['Timestamp'].dt.day
        NZERTF['hour'] = NZERTF['Timestamp'].dt.hour
        NZERTF = NZERTF.groupby(['year', 'month', 'day', 'hour']).mean()
        NZERTF['Consumption'] = NZERTF[consumers].div(1000).sum(1)
        print(temp_df['Consumption'].sum() - NZERTF['Consumption'].sum())
    return temp_df['Consumption'], NZERTF['Consumption']
        # Db.pickle_dataframe(NZERTF, f"All-Subsystems-minute-year{year}_no_redundancy.pkl")


if __name__ == '__main__':
    remove_redundant_power_consumption_from_data()
    # print(create_redundancy_dataframes()['Year2']['Load_StatusPlugLoadIron'].sum())
