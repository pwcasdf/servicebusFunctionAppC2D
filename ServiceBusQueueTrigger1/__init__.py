import logging
import azure.functions as func
from azure.iot.hub import IoTHubRegistryManager
import pickle
import requests
import pandas as pd
import numpy as np
import uuid
import os
import json
import psycopg2

def main(msg: func.ServiceBusMessage):
    registry_manager = IoTHubRegistryManager(os.environ.get('iothubConnectionString'))

    # Get message from service bus
    inputMsg=json.loads(msg.get_body().decode('utf-8'))

    # Update connection string information 
    host = os.environ.get('postgreSQL_host')
    dbname = os.environ.get('postgreSQL_dbname')
    user = os.environ.get('postgreSQL_user')
    password = os.environ.get('postgreSQL_password')
    sslmode = "require"

    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string) 
    cursor = conn.cursor()

    # Create table if there's no table for the device
    cursor.execute("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, a float, b float, c float, d float, \
        e float, f float, y float, date_time TIMESTAMP);" % inputMsg['deviceID'])

    filename1=str(uuid.uuid4())
    currentData=np.array([[inputMsg['a'], inputMsg['b'], inputMsg['c'], inputMsg['d'], inputMsg['e'], inputMsg['f']]])
    pickle.dump(currentData.reshape(-1,6), open(filename1+'.sav', 'wb'))
    r=requests.post('https://oshpoctrf.azurewebsites.net/api/oshpociot6', files={'files': open(filename1+'.sav', 'rb')  } )
    df1 = pd.read_json(r.text)
    day0_y=df1[0]["result"]
    os.remove(filename1+'.sav')

    cursor.execute("INSERT INTO %s (a, b, c, d, e, f, y, date_time) VALUES (%f, %f, %f, %f, %f, %f, %f, CURRENT_TIMESTAMP);" \
        % (inputMsg['deviceID'], inputMsg['a'], inputMsg['b'], inputMsg['c'], inputMsg['d'], inputMsg['e'], inputMsg['f'], day0_y))

    cursor.execute("SELECT (y) FROM %s ORDER BY id DESC LIMIT 5;" % inputMsg['deviceID'])

    # Counting current rows, if rows < 4 then no serial predict data produce
    cursor.execute("SELECT COUNT(id) FROM %s;" % inputMsg['deviceID'])

    if cursor.fetchall()[0][0] > 4:
        # Get previous y data
        cursor.execute("SELECT (y) FROM %s ORDER BY id DESC LIMIT 4 OFFSET 1;" % inputMsg['deviceID'])
        timeSeriesData=cursor.fetchall()

        day1_y=timeSeriesData[0][0]
        day2_y=timeSeriesData[1][0]
        day3_y=timeSeriesData[2][0]
        day4_y=timeSeriesData[3][0]

        timeSeriesData_array=np.array([[[day0_y, day1_y, day2_y, day3_y, day4_y],
            [day0_y, day1_y, day2_y, day3_y, day4_y],
            [day0_y, day1_y, day2_y, day3_y, day4_y]]])

        filename2=str(uuid.uuid4())
        pickle.dump(timeSeriesData_array.reshape(-1,3,5), open(filename2+'.sav', 'wb'))
        r=requests.post('https://oshpoctrf.azurewebsites.net/api/Timeseries_RF', files={'files': open(filename2+'.sav', 'rb')  } )
        df2 = pd.read_json(r.text)
        os.remove(filename2+'.sav')

        registry_manager.send_c2d_message(inputMsg['deviceID'], "\n" + inputMsg['deviceID'] + "\ny: " + str(day0_y) + "\n\nTime series data\n" + str(df2))
    else:
        registry_manager.send_c2d_message(inputMsg['deviceID'], "\n" + inputMsg['deviceID'] + "\ny: " + str(day0_y) + "\ndata not enough for time series data")

    # Clean up
    conn.commit()
    cursor.close()
    conn.close()

    logging.info('Python ServiceBus queue trigger processed message: %s',
                 msg.get_body().decode('utf-8'))
