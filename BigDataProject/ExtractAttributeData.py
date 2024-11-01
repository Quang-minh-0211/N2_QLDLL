import pandas as pd

df = pd.read_csv(r"/home/quangminh/BigDataProject/N2-QLDL.csv")
filterData = df.iloc[: , [3,4]].values

with open("/home/quangminh/BigDataProject/KmeansData.txt", "w") as f:
    for row in filterData:
        f.write(f"{float(row[0])}, {float(row[1])}\n")
print("Successful")

