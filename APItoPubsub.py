#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime
import json
from unidecode import unidecode
import csv
import time
from google.cloud import pubsub_v1
import requests
import json
from unidecode import unidecode

# Configuration
project_id = "model-journal-395918"
topic_id = "EnergyDemand4"
lastDate = datetime.datetime.now()
currentDate = datetime.datetime.now()
jsonl_filename = "data.jsonl"  # Nombre del archivo JSONL de salida
dataArray = []#Se ha añadido esta
dataArray2 = []
dataCheck = []
j = 0

# Create a PubSub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
#publisher = pubsub_v1.PublisherClient()
#topic_path = publisher.topic_path(project_id, topic_id)

while True:
    currentDate = datetime.datetime.now()
    difference = currentDate -lastDate
    difference = difference.total_seconds()
    if(difference >= 3600):#Volver a modificar a 3600
        currentDate = currentDate.strftime("%Y-%m-%dT%H:%M")
        lastDate = lastDate.strftime("%Y-%m-%dT%H:%M")
        url = "https://apidatos.ree.es/es/datos/generacion/estructura-generacion?start_date="+lastDate+"&end_date="+currentDate+"&time_trunc=day"
        #url = "https://apidatos.ree.es/es/datos/generacion/estructura-generacion?start_date="+"2023-09-08T00:00"+"&end_date="+"2023-09-08T12:00"+"&time_trunc=day"

        r = requests.get(url = url)
        print(type(r))
        response = r.json()
        print(response)
        for i in range(len(dataArray2)):
            dataCheck.append(dataArray2[i]['Value'])
        
        i = 0
            
        dataArray = []
        dataArray2 = []
        for el in response['included']:
            typeEn = el['type']
            values = el["attributes"]["values"][0]  # Assuming there's only one value per item
            typeRen = el["attributes"]["type"]
            time =  el["attributes"]["last-update"]
            item_value = float(values["value"])#Se ha añadido esta línea segundo intento
            if len(dataCheck) > 0:
                item_value2 = float(values["value"]) - dataCheck[j]#Se ha añadido esta-->Añadir condición cuando se cambie la producción
                
            else:
                item_value2 = values["value"]#Se ha añadido este else
            j+=1
            
            item_value = values["value"]
            dataArray.append({"Energy": typeEn, "Value": item_value2, "Renovable": typeRen, "Time": currentDate})#cambio item_value por item_value2
            dataArray2.append({"Energy": typeEn, "Value": item_value, "Renovable": typeRen, "Time": currentDate})#Se ha añadido esta línea segundo intento
        dataCheck.clear()
        print(dataCheck.clear())
        j = 0#Se ha añadido esto
        
        for entry in dataArray:
            for key, value in entry.items():
                if isinstance(value, str):
                    entry[key] = unidecode(value)
        lastDate = datetime.datetime.now()   
        message_data = json.dumps(dataArray).encode("utf-8") 
        print(type(message_data))#Volver a descomentar la línea de abojo
        print(f"Published message: {message_data}")
        future = publisher.publish(topic_path, message_data)
        future.result()
        

