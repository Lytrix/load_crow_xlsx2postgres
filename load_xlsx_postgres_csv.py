import csv
import xlrd
import requests
import requests_cache
import glob
import psycopg2
from psycopg2 import sql
from config import config  # import database credentials through database.ini
# from collections import OrderedDict

# save api requests to temporary sqlite db
requests_cache.install_cache('requests_cache', backend='sqlite')  # , expire_after=180)

# ---------
# Variables
# ---------
directory = 'data/'
schemaName = 'aanvalsplanschoon'
tableName = 'tableTest'
csvName = 'outputCSV'
data = []

# ---------
# FUNCTIONS
# ---------


def getJson(url):
    getData = requests.get(url)
    if getData.status_code == 200:
        # print(getData.status_code)
        jsonData = getData.json()
        return jsonData
    else:
        return print(getData.status_code)



def getAreaCodes(item, key, lat, lon):
    url = "https://api.data.amsterdam.nl/geosearch/search/?item=%s&lat=%s&lon=%s&radius=1" % (item, lat, lon)
    print(url)
    jsonData = getJson(url)
    # print(jsonData)
    if jsonData["features"]:
        uri = jsonData["features"][0]["properties"]["uri"]
        data = getJson(uri)
        # print(data['volledige_code'])
        return [data[key], data["stadsdeel"]["naam"]]
    else:
        print('Valt buiten Amsterdamse buurten')
        return ['Valt niet binnen buurt', 'Buiten Amsterdam']


def fillDict(worksheet, firstRow):
    Items = []
    # loop all rows on every column
    for row in range(1, worksheet.nrows):
        # print(worksheet.cell_value(row,0))
        newItem = {}
        # newItem =OrderedDict()
        newItem['Schouwronde'] = str(worksheet.cell_value(row, 0)).strip()
        newItem['Volgnummer_inspectie'] = int(worksheet.cell_value(row, 1))
        newItem['Volgnummer_score'] = int(worksheet.cell_value(row, 2))
        newItem['Aanmaakdatum_score'] = str(worksheet.cell_value(row, 3))
        newItem['Inspecteur'] = str(worksheet.cell_value(row, 4)).strip()
        newItem['Bestekspost'] = str(worksheet.cell_value(row, 5)).strip()
        newItem['Score'] = str(worksheet.cell_value(row, 6)).strip()
        nColLon = [i for i, key in enumerate(firstRow) if key.lower() in ('longitude', 'lon', 'lengtegraad')]
        if nColLon:
            newItem['lon'] = str(worksheet.cell_value(row, nColLon[0])).strip()
        else:
            newItem['lon'] = str(worksheet.cell_value(row, 7)).strip()
        nColLat = [i for i, key in enumerate(firstRow) if key.lower() in ('latitude', 'lat', 'breedtegraad')]
        if nColLat:
            newItem['lat'] = str(worksheet.cell_value(row, nColLat[0])).strip()
        else:
            newItem['lat'] = str(worksheet.cell_value(row, 8)).strip()
        areaData = getAreaCodes('buurt', 'volledige_code', newItem['lat'], newItem['lon'])
        print('Added areacode buurt')
        newItem['brtk2015'] = areaData[0]
        newItem['bc2015'] = areaData[0][:3]
        newItem['Stadsdeel'] = areaData[0][:1]
        areaData = getAreaCodes('gebiedsgerichtwerken', 'code', newItem['lat'], newItem['lon'])
        print('Added areacode GGW')
        newItem['geb22'] = areaData[0]
        newItem['name'] = areaData[1]
        newItem['Adres'] = str(worksheet.cell_value(row, 9)).strip()
        nColId = [i for i, key in enumerate(firstRow) if key.lower() == 'id']
        if nColId:
            newItem['Id'] = int(worksheet.cell_value(row, nColId[0]))
        else:
            newItem['Id'] = None
        Items.append(newItem)
        print('Added row: ' + str(len(data)))
    return Items


def createTable(schemaName, tableName):
    commands = (
        """
        DROP TABLE IF EXISTS  %s.%s
        """ % (schemaName, tableName),
        """
        CREATE TABLE %s.%s
        (
        "Schouwronde" text,
        "Volgnummer_inspectie" text,
        "Volgnummer_score" text,
        "Aanmaakdatum_score" timestamp with time zone,
        "Inspecteur" text,
        "Bestekspost" text,
        "Score" text,
        lon double precision,
        lat double precision,
        brtk2015 character varying,
        bc2015 text,
        "Stadsdeel" text,
        geb22 character varying,
        name character varying,
        "Adres" text,
        "Id" integer
        )
        """ % (schemaName, tableName)) 

    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        print('Writing to Database')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insertData(data, schemaName, tableName):
    insertQuery = sql.SQL("insert into {0}.{1} ({2}) values ({3})").format(
                          sql.Identifier(schemaName),
                          sql.Identifier(tableName),
                          sql.SQL(', ').join(map(sql.Identifier, data[0].keys())),
                          sql.SQL(', ').join(map(sql.Placeholder, data[0].keys())))
    conn = None

    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(insertQuery, list(data))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def loadXLSX(directory):
    for filename in glob.glob(directory + '*.xlsx'):
        print(filename)
        workbook = xlrd.open_workbook(filename)
        # Show sheets
        sheet_names = workbook.sheet_names()
        print(sheet_names)

        # Load each sheet
        # for i in range(0,1):
        worksheet = workbook.sheet_by_index(0)

        # Key names
        firstRow = []
        for col in range(worksheet.ncols):
            firstRow.append(worksheet.cell_value(0, col).title())
        # print(firstRow)

        # Examples
        # 'Schouwronde', 'Volgnummer Inspectie', 'Volgnummer Score', 'Aanmaakdatum Score', 'Inspecteur', 'Bestekspost', 'Score', 'Lengtegraad', 'Breedtegraad', 'Address', 'Stadsdeel', 'Mslink'
        # 'Schouwronde', 'Volgnummer Inspectie', 'Volgnummer Score', 'Aanmaakdatum Score', 'Inspecteur', 'Bestekspost', 'Score', 'Lengtegraad', 'Breedtegraad', 'Address', 'Id', 'Serienummer', 'Id-Nummer', 'Fractie', 'Containertype', 'Volume Containertype', 'Kleur', 'Well Id', 'Well Id (Customer)', 'Adres', 'Latitude', 'Longitude', 'Rd-X', 'Rd-Y', 'Wijk', 'Buurt', 'Eigenaar'
        # 'Schouwronde', 'Volgnummer Inspectie', 'Volgnummer Score', 'Aanmaakdatum Score', 'Inspecteur', 'Bestekspost', 'Score', 'Brtk2015', 'Verblijfin', 'Bc2015', 'Stadsdeel', 'Geb22', 'Name', 'Minx', 'Maxx', 'Miny', 'Maxy'

        dataSet = fillDict(worksheet, firstRow)

        data.extend(dataSet)
    return data

# ---------------------------------------------------------------------------------
# Load each xlsx and add it with fillDict to data List and save to PG Table and CSV
# ---------------------------------------------------------------------------------


if __name__ == "__main__":
    data = loadXLSX(directory)
    createTable(schemaName, tableName)
    insertData(data, schemaName, tableName)
    with open(csvName + '.csv', 'w') as f:
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        for item in data:
            # print(item)
            w.writerow(item)
