import re
# from PyDictionary import PyDictionary
from difflib import SequenceMatcher

import pandas as pd

from Project.Database import Db

for hourly in [False]:  # True, False
    for year in [1]:  # 1, 2
        time_base = "hour" if hourly else "minute"
        meta = Db.load_data(meta=True, year=year, consumption=False)

        meta.set_index("Unnamed: 0", inplace=True)

        status_condition = (lambda self: (self["Units"] == "Binary Status") &
                                         (self["Subsystem"] == "Loads") &
                                         (~self.index.str.contains("SensHeat")))
        status_columns = meta.loc[status_condition].index.tolist()
        consumer_condition = (lambda self: (self["Units"] == "W") &
                                           (self["Subsystem"].isin(["Electrical", "Loads", "Lighting"])) &
                                           (~self.index.isin(
                                               ["Elec_PowerSumpPump", "Load_ClothesWasherPowerWithStandby",
                                                "Load_DryerPowerTotal", "Elec_PowerDryer1of2",
                                                "Elec_PowerDryer2of2"])) &
                                           (~self["Description"].str.contains("emulator")))
        consumers = meta.loc[consumer_condition].index.tolist()

        matches_dict = {}
        other_dict = {}
        for col in status_columns:
            col_parts = re.findall("[A-Z]+[a-z0-9]*", col.split("_")[-1])
            for consumer in consumers:
                consumer_parts = re.findall("[A-Z]+[a-z0-9]*", consumer.split("_")[-1])
                if pd.Series(
                        [(col_part == consumer_part) for col_part in col_parts for consumer_part in
                         consumer_parts]).sum() == 2:
                    matches_dict[col] = consumer

            # if col not in matches_dict.keys():
            other_dict[col] = {}
            for consumer in consumers:
                consumer_parts = re.findall("[A-Z]+[a-z0-9]*", consumer.split("_")[-1])
                other_dict[col].update({
                    str(
                        pd.Series(
                            SequenceMatcher(a=col_part, b=consumer_part).ratio()
                            for col_part in col_parts
                            for consumer_part in consumer_parts
                        ).sum()
                    ): consumer
                })

        # for key, value in matches_dict.items():
        #     print(key)
        #     print("  ", value)

        for key, value in other_dict.items():
            print(key)
            print("  ", value[str(max(value.keys()))])
            # for s_key, s_value in value.items():
            #     print("  ", s_value)
            #     print("    ", s_key)
