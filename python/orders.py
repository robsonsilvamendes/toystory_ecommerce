import pandas as pd
import pyarrow

df = pd.read_csv("orders.csv")
df.to_parquet("orders.parquet", engine="pyarrow")

print(f'Success! Parquet file created.')
