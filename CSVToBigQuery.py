#!/usr/bin/env python
# coding: utf-8

# In[2]:


from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery
import csv
import json

# Configura la información de tu proyecto y el tema de Pub/Sub
project_id = "model-journal-395918"
subscription_id = "EnergyDemand4-sub"
archivo_csv = "datos.csv"
bucket_name = "bucketmal"
dataset_id = "dataset1Metadata"#añadido
tabla_id = "EnergyTable"#añadido

# Crea un cliente de Pub/Sub
subscriber = pubsub_v1.SubscriberClient()

# Define la función que se ejecutará cuando se reciba un mensaje
def callback(message):
    # Procesa el mensaje recibido y lo convierte a formato json
    mensaje_json = message.data.decode("utf-8")
    data = json.loads(mensaje_json)

    # Abre el archivo CSV en modo de escritura
    with open(archivo_csv, 'a', newline='') as file:
        escritor = csv.DictWriter(file, fieldnames=data[0].keys())

        # Si el archivo está vacío, escribe el encabezado
        if file.tell() == 0:
            escritor.writeheader()

        # Escribe los datos en el archivo CSV
        for fila in data:
            escritor.writerow(fila)

    # Se reconoce el mensaje y se marca como procesado
    message.ack()
    
    #Creación de un cliente do Google Cloud Storage
    storage_client = storage.Client()
    #Identificación del Bucket de Google Cloud a utilizar
    bucket = storage_client.bucket(bucket_name)
    #Creación de un archivo en el bucket con el formato requerido
    blob = bucket.blob(archivo_csv)

    # Sube el archivo CSV local al bucket
    blob.upload_from_filename(archivo_csv)

    print(f"Archivo CSV guardado en {blob.name} en el bucket {bucket_name}")

    # Crea una tabla en BigQuery y carga los datos desde el archivo CSV en el bucket
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(tabla_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    #with open(f"gs://{bucket_name}/{archivo_csv}", "rb") as source_file:
        #job = bigquery_client.load_table_from_file(
            #source_file, table_ref, job_config=job_config
        #)Estas líneas funcionaban antes

    #job.result()  # Esta también

    print(f"Datos cargados en la tabla {tabla_id} en el dataset {dataset_id}")

    table = bigquery_client.get_table(table_ref)
    
    # Itera sobre los datos y los inserta en la tabla
    rows_to_insert = []
    for fila in data:
        row = (
            fila["Energy"],
            fila["Value"],
            fila["Renovable"],
            fila["Time"]
        )
        rows_to_insert.append(row)

    #bigquery_client.insert_rows(table, rows_to_insert)esto ha sido comentado también, estaba antes

    if rows_to_insert:
        bigquery_client.insert_rows(table, [rows_to_insert[-1]])
    print(f"Datos insertados en la tabla {tabla_id}")
    ##Termina aqui

# Crea la suscripción
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Inicia la recepción de mensajes
subscriber.subscribe(subscription_path, callback=callback)

print(f"Escuchando mensajes en tiempo real. Los datos se guardan en {archivo_csv}")

# Mantén el programa en ejecución para recibir mensajes en tiempo real
import time
while True:
    time.sleep(60)  # Puedes ajustar el intervalo de tiempo si es necesario


# In[ ]:




