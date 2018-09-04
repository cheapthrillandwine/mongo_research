import pandas as pd

#変換したいJSONファイルを読み込む
df = pd.read_json(r"C:\Users\NSW00_906882\Desktop\mongo\json\None_chunk__0.json")

df.open()

#CSVに変換して任意のファイル名で保存
df.to_csv("test.csv")
