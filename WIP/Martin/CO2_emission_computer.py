import lxml.html as LH
import requests
import pandas as pd

from Project.Database import Db


# Source "https://stackoverflow.com/questions/28305578/python-get-html-table-data-by-xpath"

def text(elt):
    return elt.text_content().replace(u'\xa0', u' ')


url = "https://www.eia.gov/tools/faqs/faq.php?id=74&t=11"
r = requests.get(url)
root = LH.fromstring(r.content)

table = root.xpath("//table")
header = [text(th) for th in table[0].xpath("//th")[5:]]  # 1
data = [[text(td) for td in tr.xpath("td")]
        for tr in table[0].xpath("//tr")]  # 2
data = [row for row in data if len(row) == len(header)]  # 3
emission_data = pd.DataFrame(data, columns=header).rename(columns={" ": "Source", "pounds per kWh": "lbs/kWh"})[
    ["Source", "lbs/kWh"]]  # 4
emission_data["lbs/kWh"] = emission_data["lbs/kWh"].astype("float")

for index, source in enumerate(emission_data["Source"].tolist()):
    emission_data.loc[index, "Source"] = " ".join([
        string[0].upper() + string[1:].lower() for string in source.split(" ")])

production = Db.load_data(consumption=False, production=True).drop(
        columns=["Min Gen/Dispatch Reset", "Miscellaneous", "Demand Response", "Missing Data", "Virtual Sale at NY",
                 "Virtual Sale at MISO"]).loc[:, lambda self: self.max(0) != 0]

for col in production.columns:
    column = " ".join([
        string[0].upper() + string[1:].lower() for string in col.split(" ")])
    if column not in emission_data["Source"].tolist():
        emission_data.loc[lambda self: self.shape[0]] = [column, 0]

emission_data["g/kWh"] = emission_data["lbs/kWh"].multiply(453.592).round(2)

print(emission_data)

co2_cols = [
    "Coal",
    "Natural Gas",
    "Petroleum",
    "Waste Coal",
    "Light Oil",
    "Land Fill Gas",
    "Heavy Oil",
    "Diesel",
    "Kerosene",
    "Municipal Waste"
]

# print(production[co2_cols])
print(pd.DataFrame(pd.read_html("https://www.eia.gov/tools/faqs/faq.php?id=73&t=11")).rename(columns={"0": "Source", "1": "lbs/1e6BTU"}))
volker_tables = pd.read_html("https://www.volker-quaschning.de/datserv/CO2-spez/index_e.php", header=0, decimal=',', thousands='.')
table_1 = volker_tables[0]
table_1.rename(columns={table_1.columns[0]: "Source", table_1.columns[1]: "kg/GJ"}, inplace=True)
table_1["kg/GJ"] = table_1["kg/GJ"].astype("float")
table_1["g/kWh"] = table_1["kg/GJ"].multiply(3.6).round(2)
print(table_1)
# print(volker_tables[1].loc[(lambda self: (self.index < 10) & (~self["Fuel"].astype("str").str.contains("->")))])
