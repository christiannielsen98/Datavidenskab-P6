import eia
import pandas as pd
from Project.Database import Db

pd.options.display.max_columns = 8

api_key = "huB9HhJVZ737qVBnAxt9Ce1nIbLqtx1I1JPltZk4"
api = eia.API(api_key)

series_id_dict = {"coal": "EBA.MIDA-ALL.NG.COL.HL",
                  "hydro": "EBA.MIDA-ALL.NG.WAT.HL",
                  "natural gas": "EBA.MIDA-ALL.NG.NG.HL",
                  "nuclear": "EBA.MIDA-ALL.NG.NUC.HL",
                  "other": "EBA.MIDA-ALL.NG.OTH.HL",
                  "petroleum": "EBA.MIDA-ALL.NG.OIL.HL",
                  "solar": "EBA.MIDA-ALL.NG.SUN.HL",
                  "wind": "EBA.MIDA-ALL.NG.WND.HL",
                  }

df = pd.DataFrame()
for key, val in series_id_dict.items():
    series_search = pd.DataFrame(api.data_by_series(series=val))
    # series_search.rename(columns={series_search.columns[0]: key}, inplace=True)
    df = pd.concat(objs=(df, series_search), axis=1)
print(df.head(24))

Db.pickle_dataframe(filename="production_data.pkl", dataframe=df)