import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import re
import datetime
import numpy as np
import json
from Project.Database import Db


def json_to_dataframe(year, level):
    json_file = json.load(
        open(Db.get_save_file_directory(f"output/NZERTF_year{year}_minsup0.14_minconf_0.5/level{level}.json")))
    if "," in json_file[0]["name_node"]:
        level_df = pd.DataFrame(columns=["pattern", "supp", "conf", "time"])
        for i in json_file:
            for j in i["patterns"]:
                level_df.loc[level_df.shape[0]] = j
    else:
        level_df = pd.DataFrame(columns=["name_node", "supp", "conf"])
        for i in json_file:
            level_df.loc[level_df.shape[0]] = i
    return level_df
