## Cómo correr

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$PWD/src
mkdir -p storage reports
dagster dev -m covid_pipeline.assets -d src
# o por CLI
dagster asset materialize -m covid_pipeline.assets --select "leer_datos++"

Ver los resultados esperados
# vista rápida
head -n 20 reports/metrica_incidencia_7d.csv

# filas exactas
grep "2021-07-01,Ecuador" reports/metrica_incidencia_7d.csv
grep "2021-07-01,Peru"    reports/metrica_incidencia_7d.csv

