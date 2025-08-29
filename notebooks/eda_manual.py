import pandas as pd
from pathlib import Path

# Ruta al CSV descargado manualmente desde OWID (filtrado por Ecuador y Perú)
csv_path = Path("notebooks/owid_covid_filtered.csv")

df = pd.read_csv(csv_path)

# Perfilado requerido
perfil = {}

# Columnas y tipos
column_types = df.dtypes.astype(str).reset_index()
column_types.columns = ["columna", "tipo"]

# Mín y máx de new_cases
min_new_cases = pd.to_numeric(df["new_cases"], errors="coerce").min()
max_new_cases = pd.to_numeric(df["new_cases"], errors="coerce").max()

# % faltantes en new_cases y people_vaccinated
pct_missing_new_cases = df["new_cases"].isna().mean() * 100
pct_missing_people_vaccinated = df["people_vaccinated"].isna().mean() * 100

# Rango de fechas
df["date"] = pd.to_datetime(df["date"], errors="coerce")
min_date = df["date"].min()
max_date = df["date"].max()

# Construir tabla de perfilado
rows = []

# 1) Columnas y tipos (una fila por columna)
for _, r in column_types.iterrows():
    rows.append({
        "seccion": "columnas_tipos",
        "metro": r["columna"],
        "valor": r["tipo"],
        "notas": ""
    })

# 2) Resumenes
rows.extend([
    {"seccion":"resumen","metro":"min_new_cases","valor":min_new_cases,"notas":""},
    {"seccion":"resumen","metro":"max_new_cases","valor":max_new_cases,"notas":""},
    {"seccion":"resumen","metro":"pct_missing_new_cases","valor":pct_missing_new_cases,"notas":"%"},
    {"seccion":"resumen","metro":"pct_missing_people_vaccinated","valor":pct_missing_people_vaccinated,"notas":"%"},
    {"seccion":"resumen","metro":"min_date","valor":str(min_date.date()) if pd.notnull(min_date) else None,"notas":""},
    {"seccion":"resumen","metro":"max_date","valor":str(max_date.date()) if pd.notnull(max_date) else None,"notas":""},
])

perfilado = pd.DataFrame(rows)
perfilado.to_csv("reports/tabla_perfilado.csv", index=False)
print("Perfilado guardado en reports/tabla_perfilado.csv")
