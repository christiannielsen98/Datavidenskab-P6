import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import re
import datetime
import numpy as np
import json
from Project.Database import Db

level1_json = json.load(
    open(Db.get_project_path("Project/_04TPMAlgorithm/TPM/output/NZERTF_year1_minsup0.14_minconf_0.5/level1.json")))

level2_json = json.load(
    open(Db.get_project_path("Project/_04TPMAlgorithm/TPM/output/NZERTF_year1_minsup0.14_minconf_0.5/level2.json")))

level3_json = json.load(
    open(Db.get_project_path("Project/_04TPMAlgorithm/TPM/output/NZERTF_year1_minsup0.14_minconf_0.5/level3.json")))

def json_to_dataframe(filename):
    json_file = json.load(open(filename))
    if "," in json_file[0]["name_node"]:
        level_df = pd.DataFrame(columns=["pattern","supp","conf", "time"])
        for i in json_file:
            for j in i["patterns"]:
                level_df.loc[level_df.shape[0]] = j
    else:
        level_df = pd.DataFrame(columns=["name_node","supp","conf"])
        for i in json_file:
            level_df.loc[level_df.shape[0]] = i
    return level_df