import pandas as pd

from Project.Database import Db
from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import find_average_power_consumption_per_minute
from Project._05InferKnowledgeOfRules.infer_rules_functions import SE_time_df, json_to_dataframe


def find_interval(quantile=0.9):
    time_delta_dict = {}
    for year in [1, 2]:
        time_delta_dict[f'year{year}'] = time_delta_dict.get(f'year{year}', {})
        for index, row in json_to_dataframe(year=year, level=1, with_redundancy=False).iterrows():
            if 'SensHeat' not in row['pattern']:
                time_delta_dict[f'year{year}'][row['pattern']] = pd.Series(dtype='float64')
                for intervals in row["time"].values():
                    for interval in intervals:
                        time_delta_dict[f'year{year}'][row['pattern']] = pd.concat(
                            objs=(time_delta_dict[f'year{year}'][row['pattern']],
                                  pd.Series((pd.to_datetime(interval[1], format="%Y-%m-%d %H:%M:%S") -
                                             pd.to_datetime(interval[0],
                                                            format="%Y-%m-%d %H:%M:%S")).total_seconds() / 60)),
                            ignore_index=True)
                time_delta_dict[f'year{year}'][row['pattern']] = round(time_delta_dict[f'year{year}'][
                    row['pattern']].quantile(quantile), 0)
        time_delta_dict[f'year{year}'] = pd.Series(time_delta_dict[f'year{year}'])
    return pd.DataFrame(time_delta_dict)


print(find_interval())
print(find_average_power_consumption_per_minute())

NZERTF, meta, production = Db.load_data(meta=True, production=True, year=1, with_redundancy=False, hourly=False)

standby_power = meta.loc[(lambda self: (~self['Standby_Power'].isna()) & (self['Standby_Power'] > 0)), 'Standby_Power']

energy_needed_dict = {}
for app in apps:
    if 'SensHeat' not in app:
        if app in average_power_consumption_per_minute.index:
            energy_needed_dict[app] = f'{average_power_consumption_per_minute.loc[app][0] * describe_intervals(app)["75%"] / 60000} kWh'

energy_needed_df = pd.DataFrame(energy_needed_dict.items(),columns=['appliance', 'energy_need'])
