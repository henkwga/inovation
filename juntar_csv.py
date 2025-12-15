import os
import pandas as pd

pasta = r"C:\Users\luigi\Documents\! PJ\Hackaton"

arquivos = [f for f in os.listdir(pasta) if f.endswith(".csv")]

dfs = []

for arquivo in arquivos:
    caminho = os.path.join(pasta, arquivo)
    df = pd.read_csv(caminho)

    if len(dfs) > 0:
        if list(df.columns) != list(dfs[0].columns):
            print(f"o arquivo {arquivo} tem cabeçalho diferente, ignorado.")
            continue

    dfs.append(df)

df_final = pd.concat(dfs, ignore_index=True)
df_final.to_csv(r"inovation\resultado.csv", index=False)

print("deu certo boy")
