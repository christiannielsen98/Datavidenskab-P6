import pandas as pd

from Project.Database import Db
from Project._03FeatureSelection.NZERTF_dataframe_redundancy_functions import find_average_power_consumption_per_minute
from Project._05InferKnowledgeOfRules.infer_rules_functions import SE_time_df, json_to_dataframe


print(SE_time_df(json_to_dataframe(year=1, level=1, with_redundancy=False)))
print(find_average_power_consumption_per_minute())

NZERTF, meta, production = Db.load_data(meta=True, production=True, year=1, with_redundancy=False, hourly=False)

standby_power = meta.loc[(lambda self: (~self['Standby_Power'].isna()) & (self['Standby_Power'] > 0)), 'Standby_Power']

energy_needed_dict = {}
for app in apps:
    if 'SensHeat' not in app:
        if app in average_power_consumption_per_minute.index:
            energy_needed_dict[app] = f'{average_power_consumption_per_minute.loc[app][0] * describe_intervals(app)["75%"] / 60000} kWh'

energy_needed_df = pd.DataFrame(energy_needed_dict.items(),columns=['appliance', 'energy_need'])
