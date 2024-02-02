import pandas as pd

from splink.database_api import DuckDBAPI
from splink.linker import Linker

df = pd.read_csv("people.csv")
linker = Linker(df, "path/to/model.json", DuckDBAPI())

linker.predict()
