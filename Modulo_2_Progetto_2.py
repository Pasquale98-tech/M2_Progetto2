import pandas as pd
import numpy as np
import json
import random

# PARTE 1 — CREAZIONE DATASET

#1. Ordini.csv: 100.000 righe con ClienteID, ProdottoID, Quantità e DataOrdine.
n_ordini = 100000

ordini = pd.DataFrame({
    "ClienteID": np.random.randint(1, 5001, n_ordini),
    "ProdottoID": np.random.randint(1, 21, n_ordini),
    "Quantità": np.random.randint(1, 10, n_ordini),
    "DataOrdine": np.random.choice(
        pd.date_range("2025-01-01", "2025-12-31"),
        n_ordini
    )
})

ordini.to_csv("ordini.csv", index=False)

#2. prodotti.json: 20 prodotti con Categoria e Fornitore.

categorie = ["Abbigliamento", "Elettronica", "Casa", "Sport"]
fornitori = ["FornitoreA", "FornitoreB", "FornitoreC"]

prodotti = []

for i in range(1, 21):
    prodotti.append({
        "ProdottoID": i,
        "NomeProdotto": f"Prodotto_{i}",
        "Categoria": random.choice(categorie),
        "Fornitore": random.choice(fornitori),
        "Prezzo": round(np.random.uniform(5, 200), 2)
    })

with open("prodotti.json", "w") as f:
    json.dump(prodotti, f)


#3. clienti.csv: 5.000 clienti con Regione e Segmento.

regioni = ["Nord", "Centro", "Sud"]
segmenti = ["B2B", "Alto spendenti", "Fedeli", "Nuovi"]

clienti = pd.DataFrame({
    "ClienteID": range(1, 5001),
    "Regione": np.random.choice(regioni, 5000),
    "Segmento": np.random.choice(segmenti, 5000)
})

clienti.to_csv("clienti.csv", index=False)

print("Dataset creati correttamente.")

# PARTE 2 — CREAZIONE DATAFRAME UNIFICATO

ordini = pd.read_csv("ordini.csv")
prodotti = pd.read_json("prodotti.json")
clienti = pd.read_csv("clienti.csv")

df = ordini.merge(prodotti, on="ProdottoID", how="left")

df = df.merge(clienti, on="ClienteID", how="left")

print("\nDataFrame unificato:")
print(df.head())

# PARTE 3 — OTTIMIZZAZIONE

print("\nMemoria prima ottimizzazione:")
print(df.memory_usage(deep=True).sum() / 1024**2, "MB")

df["ClienteID"] = df["ClienteID"].astype("int32")
df["ProdottoID"] = df["ProdottoID"].astype("int16")
df["Quantità"] = df["Quantità"].astype("int8")
df["Prezzo"] = df["Prezzo"].astype("float32")

df["Categoria"] = df["Categoria"].astype("category")
df["Fornitore"] = df["Fornitore"].astype("category")
df["Regione"] = df["Regione"].astype("category")
df["Segmento"] = df["Segmento"].astype("category")

df["DataOrdine"] = pd.to_datetime(df["DataOrdine"])

print("\nMemoria dopo ottimizzazione:")
print(df.memory_usage(deep=True).sum() / 1024**2, "MB")

# PARTE 4 — COLONNE CALCOLATE E FILTRI

df["ValoreTotale"] = df["Prezzo"] * df["Quantità"]

df_filtrato = df[df["ValoreTotale"] > 100]

print("\nOrdini con ValoreTotale > 100:")
print(df_filtrato.head())

print("\nNumero ordini filtrati:", len(df_filtrato))

# AGGREGAZIONE DATI

analisi = (
    df_filtrato
    .groupby(["Regione", "Categoria"])["ValoreTotale"]
    .sum()
    .reset_index()
    .sort_values(by="ValoreTotale", ascending=False)
)

print("\nAnalisi fatturato per Regione e Categoria:")
print(analisi.head())