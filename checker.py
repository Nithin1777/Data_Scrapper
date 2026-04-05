import pandas as pd
df = pd.read_csv("robu_progress.csv")
print(df['status'].value_counts())
print(f"Last 5 entries:")
print(df.tail())