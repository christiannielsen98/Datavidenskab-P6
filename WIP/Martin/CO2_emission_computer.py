import lxml.html as LH
import requests
import pandas as pd

from Project.Database import Db

#Source "https://stackoverflow.com/questions/28305578/python-get-html-table-data-by-xpath"

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
data = pd.DataFrame(data, columns=header).rename(columns={" ": "Source", "pounds per kWh": "PoundsPerkWh"})[
    ["Source", "PoundsPerkWh"]]  # 4
Db.pickle_dataframe(dataframe=data, filename="")
