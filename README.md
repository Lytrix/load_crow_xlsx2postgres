# import CROW XLSX 2 Postgres & CSV #

Python script to load multiple XLSX files into one dictonary 
with addition of 'Stadsdeel','Buurt and 'Gebiedsgerichtwerken' areas
and save them to a Postgres Table and CSV file.


### Install procedure ###

```
git clone https://github.com/lytrix/load_crow_xlsx2postgres.git
virtualenv --python=$(which python3) venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
change database.ini.example to database.ini with own database settings

```
python load_xlsx_postgres_csv.py

```
