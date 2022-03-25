import re
# from PyDictionary import PyDictionary
from difflib import SequenceMatcher

import pandas as pd

from Project.Database import Db


def replace_short_remove_fill(string):
    if "MBR" in string:
        string = string.replace("MBR", "Masterbedroom")
    elif "MBA" in string:
        string = string.replace("MBA", "Masterbathroom")
    elif "BR" in string:
        string = string.replace("BR", "Bedroom")
    elif "BA" in string:
        string = string.replace("BA", "Bathroom")
    elif "LR" in string:
        string = string.replace("LR", "Livingroom")
    if "Appliance" in string:
        string = string.replace("Appliance", "")
    if "Load" in string:
        string = string.replace("Load", "")
    if "Status" in string:
        string = string.replace("Status", "")
    if "Power" in string:
        string = string.replace("Power", "")
    if "Usage" in string:
        string = string.replace("Usage", "")
    if "Plugs" in string:
        string = string.replace("Plugs", "Plug")
    return string

def direct_matcher(status_parts, matches_dict, sub_subsystems):
    for consumer_orig, consumer_row in meta.loc[
        lambda self: consumer_condition(self, sub_subsystems) & location_condition(
            self)].iterrows():
        consumer = replace_short_remove_fill(consumer_orig)
        consumer_parts = re.findall("[A-Z0-9]+[a-z0-9]*", consumer.split("_")[-1])
        if ("Light" in status and "Light" in consumer) or not ("Light" in status or "Light" in consumer):
            if ("Plug" in status and "Plug" in consumer) or not ("Plug" in status or "Plug" in consumer):
                if pd.Series(
                        [(status_part == consumer_part) for status_part in status_parts for
                         consumer_part in consumer_parts]).sum() == 2:
                    matches_dict[status_orig] = ["".join(status_parts), "".join(consumer_parts), consumer_orig]
                elif pd.Series(
                        [(status_part == consumer_part) for status_part in status_parts for
                         consumer_part in consumer_parts]).sum() == 1:
                    matches_dict[status_orig] = ["".join(status_parts), "".join(consumer_parts), consumer_orig]
    return matches_dict


for hourly in [False]:  # True, False
    for year in [1]:  # 1, 2
        time_base = "hour" if hourly else "minute"
        meta = Db.load_data(meta=True, year=year, consumption=False)

        meta.set_index("Unnamed: 0", inplace=True)

        status_condition = (lambda self: (self["Units"] == "Binary Status") &
                                         (self["Subsystem"] == "Loads") &
                                         (~self.index.str.contains("SensHeat")))
        status_attributes = meta.loc[status_condition]
        consumer_condition = (lambda self, subsystems: (self["Units"] == "W") &
                                                       (self["Subsystem"].isin(subsystems)) &
                                                       (~self.index.isin(
                                                           ["Elec_PowerSumpPump", "Load_ClothesWasherPowerWithStandby",
                                                            "Load_DryerPowerTotal", "Elec_PowerDryer1of2",
                                                            "Elec_PowerDryer2of2"])) &
                                                       (~self["Description"].str.contains("emulator")))

        matches_dict = {}
        other_dict = {}
        for status_orig, status_row in status_attributes.iterrows():
            status_room = status_row["Measurement_Location"]
            status = replace_short_remove_fill(status_orig)

            location_condition = (
                lambda self: self["Measurement_Location"].isin([status_row["Measurement_Location"], "Multiple"]))

            status_parts = re.findall("[A-Z]+[a-z0-9]*", status.split("_")[-1])

            matches_dict.update(direct_matcher(status_parts, matches_dict, ["Electrical", "Lighting"]))

            if status_orig not in matches_dict.keys():
                matches_dict.update(direct_matcher(status_parts, matches_dict, ["Loads", "Load"]))

            # other_dict[status_orig] = {}
            # for consumer_orig in consumers.index:
            #     consumer = consumer_orig
            #     if "MBR" in consumer:
            #         consumer = consumer.replace("MBR", "Masterbedroom")
            #     elif "MBA" in consumer:
            #         consumer = consumer.replace("MBA", "Masterbathroom")
            #     elif "BR" in consumer:
            #         consumer = consumer.replace("BR", "Bedroom")
            #     elif "BA" in consumer:
            #         consumer = consumer.replace("BA", "Bathroom")
            #     elif "LR" in consumer:
            #         consumer = consumer.replace("LR", "Livingroom")
            #     consumer_parts = re.findall("[A-Z]+[a-z0-9]*", consumer.split("_")[-1])
            #     consumer_parts.remove("Power")
            #     if status_room == consumer_room or consumer_room == "Multiple":
            #         if not ("Light" in status or "Light" in consumer):
            #                 other_dict[status_orig].update({
            #                     str(pd.Series(
            #                             SequenceMatcher(a=status_part, b=consumer_part).ratio()
            #                             for status_part in status_parts
            #                             for consumer_part in consumer_parts).mean()
            #                     ): ["".join(status_parts), "".join(consumer_parts), consumer_orig]
            #                 })

        for key, value in matches_dict.items():
            print(key)
            print("  ", value)

        missing_statuses = [status for status in status_attributes.index if status not in matches_dict.keys()]
        print(missing_statuses)
        #     print(key)
        #     print("  ", value[str(max(value.keys()))])
        # for s_key, s_value in value.items():
        #     print("  ", s_value)
        #     print("    ", s_key)
