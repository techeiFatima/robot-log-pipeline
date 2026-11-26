import glob
import duckdb
import pandas as pd

path = glob.glob("data/raw/*.xlsx")[0]
df = pd.read_excel(path)

print("Rows, columns:", df.shape)
print("Columns:", df.columns.tolist())
print("Missing values per column:")
print(df.isna().sum())
print("Duplicate rows:", df.duplicated().sum())

con = duckdb.connect("cobot.duckdb")
con.execute("CREATE OR REPLACE TABLE raw_logs AS SELECT * FROM df")
print("Loaded into table raw_logs")
