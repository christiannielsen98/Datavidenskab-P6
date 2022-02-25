import re

from Project.Database import Db

year1_hour_meta = Db.load_data(consumption=False, meta=True)
column_names = {}
column_name_bigrams = {}
standby_names = {}
standby_name_bigrams = {}

for attr in year1_hour_meta["Unnamed: 0"]:
    if "Standby" in attr and "Power" in attr:
        standby_names.update({attr: re.findall("[A-Z0-9]+[^A-Z]*", attr.split("_")[-1])})
        standby_name_bigrams.update({attr: zip(standby_names[attr][:-1], standby_names[attr][1:])})
    if "Standby" not in attr and "Power" in attr:
        column_names.update({attr: re.findall("[A-Z0-9]+[^A-Z]*", attr.split("_")[-1])})
        for bigram in zip(column_names[attr][:-1], column_names[attr][1:]):
            print("".join(bigram))
            column_name_bigrams[bigram] = column_name_bigrams.get(bigram, list()).append(attr)

for key, value in standby_name_bigrams.items():
    for bigram in value:
        if bigram in column_name_bigrams[key]:
            print(key, value)


