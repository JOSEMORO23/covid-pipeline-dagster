from __future__ import annotations
import io
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests
from dagster import (
    asset, asset_check, AssetCheckResult, AssetCheckSeverity,
    Definitions
)
from dagster import FilesystemIOManager  # reemplaza fs_io_manager

# -----------------------
# Utilidades
# -----------------------
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas según el esquema de OWID recibido."""
    ren = {}
    if "location" not in df.columns and "country" in df.columns:
        ren["country"] = "location"
    if "iso_code" not in df.columns and "code" in df.columns:
        ren["code"] = "iso_code"
    if ren:
        df = df.rename(columns=ren)
    return df

def _require_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    """Devuelve columnas faltantes (no levanta excepción)."""
    return [c for c in cols if c not in df.columns]

# Config
OWID_URL = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
COMPARISON_COUNTRY = "Peru"
TARGET_COUNTRIES = {"Ecuador", COMPARISON_COUNTRY}

# -----------------------
# Paso 2: Lectura sin transformar
# -----------------------
@asset(group_name="ingesta", description="Lee CSV canónico de OWID sin transformar.")
def leer_datos(context) -> pd.DataFrame:
    resp = requests.get(OWID_URL, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    df = _normalize_columns(df)
    context.log.info(f"Filas leídas: {len(df):,}; columnas: {list(df.columns)[:10]}…")
    return df

# -------- Chequeos de ENTRADA --------
@asset_check(asset=leer_datos, description="No hay fechas futuras.")
def check_no_fechas_futuras(context, leer_datos: pd.DataFrame) -> AssetCheckResult:
    if "date" not in leer_datos.columns:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"error": "Falta columna 'date'"}
        )
    now = datetime.now(timezone.utc).date()
    fechas = pd.to_datetime(leer_datos["date"], errors="coerce").dt.date
    filas_afectadas = int((fechas > now).sum())
    return AssetCheckResult(
        passed=(filas_afectadas == 0),
        # si pasa, no seteamos severity; si falla, ERROR
        severity=AssetCheckSeverity.ERROR if filas_afectadas else None,
        metadata={"filas_afectadas": filas_afectadas, "hoy": str(now)},
    )

@asset_check(asset=leer_datos, description="Claves no nulas; unicidad (location,date); population>0.")
def check_campos_y_unicidad(context, leer_datos: pd.DataFrame) -> AssetCheckResult:
    faltan = _require_cols(leer_datos, ["location", "date", "population"])
    if faltan:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"faltan_columnas": ", ".join(faltan)}
        )
    base = leer_datos.copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce")
    n_null_loc = int(base["location"].isna().sum())
    n_null_date = int(base["date"].isna().sum())
    n_null_pop = int(base["population"].isna().sum())
    dup = int(base.duplicated(subset=["location", "date"]).sum())
    pop_le_0 = int((pd.to_numeric(base["population"], errors="coerce") <= 0).sum())
    passed = (n_null_loc==0 and n_null_date==0 and n_null_pop==0 and dup==0 and pop_le_0==0)
    return AssetCheckResult(
        passed=passed,
        severity=None if passed else AssetCheckSeverity.ERROR,
        metadata={
            "null_location": n_null_loc,
            "null_date": n_null_date,
            "null_population": n_null_pop,
            "duplicados_location_date": dup,
            "population_le_0": pop_le_0,
        },
    )

@asset_check(asset=leer_datos, description="new_cases no negativos (revisiones OWID).")
def check_new_cases_no_negativos(context, leer_datos: pd.DataFrame) -> AssetCheckResult:
    if "new_cases" not in leer_datos.columns:
        return AssetCheckResult(
            passed=True,  # no bloquear
            severity=AssetCheckSeverity.WARN,
            metadata={"nota": "Falta columna 'new_cases' para el check"}
        )
    serie = pd.to_numeric(leer_datos["new_cases"], errors="coerce")
    n_neg = int((serie < 0).sum())
    # No bloquear el run: marcar como passed con warning si hay negativos
    return AssetCheckResult(
        passed=True,
        severity=AssetCheckSeverity.WARN,
        metadata={"new_cases_negativos": n_neg, "nota": "OWID puede tener revisiones negativas puntuales."},
    )

# -----------------------
# Paso 3: Procesamiento
# -----------------------
@asset(group_name="procesamiento", description="Filtra países; quita nulos/duplicados; selecciona columnas.")
def datos_procesados(context, leer_datos: pd.DataFrame) -> pd.DataFrame:
    faltan = _require_cols(leer_datos, ["location", "date", "new_cases", "people_vaccinated", "population"])
    if faltan:
        raise KeyError(f"Faltan columnas requeridas para procesar: {faltan}")
    df = leer_datos.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["location"].isin(TARGET_COUNTRIES)].copy()
    df = df[~(df["new_cases"].isna() | df["people_vaccinated"].isna())].copy()
    before = len(df)
    df = df.drop_duplicates(subset=["location", "date"])
    removed_dups = before - len(df)
    df = df[["location", "date", "new_cases", "people_vaccinated", "population"]].copy()
    context.log.info(f"Filas tras limpieza: {len(df):,}; duplicados removidos: {removed_dups}")
    return df

# -----------------------
# Paso 4: Métricas
# -----------------------
@asset(group_name="metricas", description="Incidencia 7d por 100k.")
def metrica_incidencia_7d(context, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy().sort_values(["location", "date"])
    df["incidencia_diaria"] = (pd.to_numeric(df["new_cases"], errors="coerce") /
                               pd.to_numeric(df["population"], errors="coerce")) * 100000
    df["incidencia_7d"] = df.groupby("location")["incidencia_diaria"].transform(
        lambda s: s.rolling(7, min_periods=7).mean()
    )
    out = df[["date", "location", "incidencia_7d"]].dropna().rename(columns={"date": "fecha", "location": "pais"})
    return out

@asset(group_name="metricas", description="Factor de crecimiento 7d: suma 7d / suma previos 7d.")
def metrica_factor_crec_7d(context, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy().sort_values(["location", "date"])
    df["new_cases"] = pd.to_numeric(df["new_cases"], errors="coerce")
    df["casos_semana"] = df.groupby("location")["new_cases"].transform(lambda s: s.rolling(7, min_periods=7).sum())
    df["casos_semana_prev"] = df.groupby("location")["new_cases"].transform(lambda s: s.shift(7).rolling(7, min_periods=7).sum())
    df["factor_crec_7d"] = df["casos_semana"] / df["casos_semana_prev"]
    out = df[["date", "location", "casos_semana", "factor_crec_7d"]].dropna().rename(
        columns={"date": "semana_fin", "location": "pais"}
    )
    return out

# -----------------------
# Paso 5: Chequeo de salida
# -----------------------
@asset_check(asset=metrica_incidencia_7d, description="Incidencia 7d en [0,2000] (solo informativo).")
def check_incidencia_rango(context, metrica_incidencia_7d: pd.DataFrame) -> AssetCheckResult:
    if "incidencia_7d" not in metrica_incidencia_7d.columns:
        return AssetCheckResult(
            passed=True,  # no bloquear
            severity=AssetCheckSeverity.WARN,
            metadata={"nota": "Falta 'incidencia_7d' para el check"}
        )
    serie = pd.to_numeric(metrica_incidencia_7d["incidencia_7d"], errors="coerce")
    fuera = int(((serie < 0) | (serie > 2000)).sum())
    # No bloquear el run: siempre passed; avisar si hay fuera de rango
    return AssetCheckResult(
        passed=True,
        severity=AssetCheckSeverity.WARN,
        metadata={"valores_fuera_de_rango": fuera, "rango_referencia": "[0,2000]"},
    )

# -----------------------
# Paso 6: Exportación
# -----------------------
@asset(group_name="reporte", description="Exporta resultados a /reports en Excel + CSVs.")
def reporte_excel_covid(context,
                        datos_procesados: pd.DataFrame,
                        metrica_incidencia_7d: pd.DataFrame,
                        metrica_factor_crec_7d: pd.DataFrame) -> str:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    excel_path = reports_dir / "reporte_covid.xlsx"
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        datos_procesados.to_excel(writer, index=False, sheet_name="datos_procesados")
        metrica_incidencia_7d.to_excel(writer, index=False, sheet_name="metrica_incidencia_7d")
        metrica_factor_crec_7d.to_excel(writer, index=False, sheet_name="metrica_factor_crec_7d")
    datos_procesados.to_csv(reports_dir / "datos_procesados.csv", index=False)
    metrica_incidencia_7d.to_csv(reports_dir / "metrica_incidencia_7d.csv", index=False)
    metrica_factor_crec_7d.to_csv(reports_dir / "metrica_factor_crec_7d.csv", index=False)
    context.log.info(f"Reporte exportado a {excel_path}")
    return str(excel_path)

# Definitions con IO manager persistente
defs = Definitions(
    assets=[leer_datos, datos_procesados, metrica_incidencia_7d, metrica_factor_crec_7d, reporte_excel_covid],
    asset_checks=[check_no_fechas_futuras, check_campos_y_unicidad, check_new_cases_no_negativos, check_incidencia_rango],
    resources={"io_manager": FilesystemIOManager(base_dir="storage")},
)
