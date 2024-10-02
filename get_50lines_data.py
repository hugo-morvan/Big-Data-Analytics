import pandas as pd

data = pd.read_csv("data/precipitation-readings.csv", header=None)
print(data.head(50))