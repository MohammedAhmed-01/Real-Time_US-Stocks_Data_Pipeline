import pandas as pd

df = pd.read_csv("metrics_all_models.csv")

print(df.shape)
print(df.columns.tolist())

print("\nRMSE:")
print(df["rmse"].describe())

print("\nMAE:")
print(df["mae"].describe())

print("\nLargest RMSE:")
print(df.nlargest(10, "rmse")[["ticker", "model", "rmse", "mae"]])