import pandas as pd

from Project.Database import Db

status_condition = (lambda self: (
        (self["Subsystem"] == "Loads") &
        (self["Parameter"] == "Status_OnOff")
))


def location_count(meta_data):
    return meta_data.loc[lambda self: (self.index.str.contains("Light"))].groupby('Measurement_Location').size()


def light_location_dict(meta_data):
    return_dict = {}
    location_count_series = location_count(meta_data)
    for location, size in location_count_series[location_count_series > 1].iteritems():
        return_dict.update({'Load_Status' + location + 'Lights': meta_data.loc[
            (lambda self:
             (self.index.str.contains("Light")) &
             (self['Measurement_Location'] == location))].index.tolist()})
    return return_dict


def create_column_translator(meta_data):
    return {old_col: new_col for new_col, old_col_list in light_location_dict(meta_data).items() for old_col in
            old_col_list}


def transform_for_TPM(year):
    data, meta_data = Db.load_data(hourly=False, meta=True, year=year)
    meta_data = meta_data.loc[status_condition]
    data['DayOfYear'] = data['Timestamp'].dt.dayofyear
    data['DayIndex'] = data['DayOfYear'] - data['DayOfYear'][0]

    # Find the first row index with negative value
    first_index = data.loc[lambda self: self['DayIndex'] < 0].index.tolist()[0]
    data.loc[first_index:, 'DayIndex'] = data.loc[first_index:, 'DayIndex'] + 365
    data['Timestamp'] = (pd.DatetimeIndex(data["Timestamp"]).astype(int) / 1e9).astype(int)
    data = data[['Timestamp', 'DayIndex'] + meta_data.index.tolist()]
    appliance_list = meta_data.index.tolist()
    # Group room lights
    for new_col, old_cols in light_location_dict(meta_data).items():
        data[new_col] = data[old_cols].max()
        data.drop(old_cols, axis=1, inplace=True)
        appliance_list.append(new_col)
        for old_col in old_cols:
            appliance_list.remove(old_col)

    # Find appliance switch on/off timestamps
    tmp_list = []
    for appliance in appliance_list:
        appliance_switch = data.loc[(lambda self: self[appliance] != self[appliance].shift(1)),
                                    ['Timestamp', 'DayIndex', appliance]]
        switch_index_list = appliance_switch.index.tolist()
        for index, switch_index in enumerate(switch_index_list):
            if appliance_switch.loc[switch_index, appliance]:
                try:
                    tmp_list.append({
                        "start": appliance_switch.loc[switch_index, "Timestamp"],
                        "end": appliance_switch.loc[switch_index_list[index + 1], "Timestamp"],
                        "appliance": appliance,
                        "day": appliance_switch.loc[switch_index, "DayIndex"]
                    })
                except:
                    continue

    csv_data = pd.DataFrame(tmp_list).sort_values(by=['day', 'start']).reset_index(drop=True)
    csv_data.to_csv(path_or_buf=Db.get_project_path(f'Project/04TPMAlgorithm/TPM/Data/NZERTF_year{year}'),
                    header=False,
                    index=False)


if __name__ == '__main__':
    for year in [1, 2]:
        transform_for_TPM(year)
