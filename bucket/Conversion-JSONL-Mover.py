import os 
from google.cloud import storage
import xmltodict 
import json 



os.environ['GOOGLE_APPLICATION_CREDENTIALS']='cuentadeservicio.json'
storage_client = storage.Client()
print("Metodos permitidos sobre el bucket",dir(storage_client))

for bucket in storage_client.list_buckets(max_results=100):
    print("\nListado de buckets permitidos",bucket)



#Acceder a bucket espeifico y sus detalles 
#print("\nDetalles del bucket emitidos")
#my_bucket = storage_client.get_bucket('documentos-tributarios-emitidos-historicos')


####Funcion para subir archivos
def upload_to_bucket(blob_name, file_path, bucket_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob= bucket.blob(blob_name)
        blob.upload_from_file_name(file_path)
        return True
    
    except Exception as e:
        print(e)
        return False

#Funcion para recorrer archivos del bucket, el prefix sirve para meterse en los directorios
def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    destination_bucket_name='documentos-tributarios-recibidos-historicos-leidos-movidos'
    for blob in blobs:
        y=blob.name
        x1=y.replace(".xml",".json")
        bt= blob.download_as_string()
        if((bt)!=b''):
            data = xmltodict.parse(bt)
            with open(x1, 'w') as f:
                json.dump(data, f)
            upload_blob(bucket_name, x1,"documentos-tributarios-recibidos-historicos-formato-json")
            move_blob(bucket_name,y, destination_bucket_name, y)
        else:
            None
        
    """En caso de tener subcarpetas usar esta funcion"""
    """if delimiter:
        print('Prefixes:')
        for prefix in blobs.prefixes:
            print(prefix)"""

#Renombrar archivos en bucket
def rename_blob(bucket_name, blob_name, new_blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    new_blob = bucket.rename_blob(blob, new_blob_name)
    return new_blob.public_url


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(destination_blob_name)
    blob = bucket.blob(source_file_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def list_blobs_with_prefix_json(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    for blob in blobs:
        print(blob.name)



def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )

list_blobs_with_prefix("documentos-tributarios-recibidos-historicos-leidos",None)
#list_blobs_with_prefix_json("documentos-tributarios-recibidos-to-json",None)





