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

def main(msg: func.ServiceBusMessage):
    registry_manager = IoTHubRegistryManager(os.environ.get('iothubConnectionString'))

    inputMsg=json.loads(msg.get_body().decode('utf-8'))

    filename=str(uuid.uuid4())
    data=np.array([[inputMsg['a'], inputMsg['b'], inputMsg['c'], inputMsg['d'], inputMsg['e'], inputMsg['f']]])
    pickle.dump(data.reshape(-1,6), open(filename+'.sav', 'wb'))
    r=requests.post('', files={'files': open(filename+'.sav', 'rb')  } )
    df = pd.read_json(r.text)
    result=df[0]["result"]
    os.remove(filename+'.sav')
    df.head()

    registry_manager.send_c2d_message(inputMsg['deviceID'], json.dumps(result))
    logging.info('Python ServiceBus queue trigger processed message: %s',
                 msg.get_body().decode('utf-8'))

