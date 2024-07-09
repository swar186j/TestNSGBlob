import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import json
import os
from urllib.parse import unquote

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="insights-logs-networksecuritygroupflowevent/{resourceId}/y={year}/m={month}/d={day}/h={hour}/m={minute}/macAddress={macAddress}/PT1H.json",
                  connection="nsgflowlogsamp_STORAGE") 
def nsg_blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob\n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AzureWebJobsStorage"))

    try:
        # Extract container and blob names from URI
        uri_parts = myblob.uri.split('/')
        container_name = uri_parts[3]
        blob_name = '/'.join(uri_parts[4:])
        decoded_blob_name = unquote(blob_name)  # Decode the blob name to handle URL-encoded characters

        logging.info(f"Container name: {container_name}")
        logging.info(f"Blob name: {decoded_blob_name}")

        # Read the checkpoint (if exists) from blob metadata
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=decoded_blob_name)
        properties = blob_client.get_blob_properties()
        metadata = properties.metadata
        logging.info(f"metadat +++++ {metadata}")
        start_index = int(metadata.get("start_index", "0"))
        logging.info(f"Starting index from metadata: {start_index}")

        # Read the JSON blob
        blob_content = myblob.read().decode('utf-8')
        logs = json.loads(blob_content)
        logging.info(f"Total logs read: {len(logs)}")

        # Process new logs starting from the checkpoint
        new_logs = logs["records"][start_index:]
        for log in new_logs:
            logging.info(f"Processing log: {log}")
            start_index += 1
        logging.info(f"start index ===== {start_index}")

        # Update the checkpoint in metadata
        metadata["start_index"] = str(start_index)
        blob_client.set_blob_metadata(metadata)
        logging.info(f"set blob ----- {blob_client.set_blob_metadata(metadata)}")
        logging.info(f"Checkpoint updated to index: {start_index}")

        #verify metadata is updated
        updated_properties = blob_client.get_blob_properties()
        updated_metadata = updated_properties.metadata
        logging.info(f"updated metadata ***** {updated_metadata}")

    except Exception as e:
        logging.error(f"Error processing blob: {e}")
