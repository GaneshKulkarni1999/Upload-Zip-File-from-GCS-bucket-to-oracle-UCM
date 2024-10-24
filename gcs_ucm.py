import os
import base64
import requests
from google.cloud import storage
from google.cloud import pubsub_v1
from requests.auth import HTTPBasicAuth
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import zipfile
import io

# Set up Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Ganesh Kulkarni\Desktop\ucm.json"
os.environ['GOOGLE_CLOUD_PROJECT'] = 'nodal-empire-426212-c4'  # Replace with your actual project ID


class ListFiles(beam.DoFn):
    def __init__(self, bucket_name, prefix):
        self.bucket_name = bucket_name
        self.prefix = prefix

    def process(self, element):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.prefix)
        for blob in blobs:
            if blob.name.endswith('.csv'):
                yield blob.name

class DownloadFile(beam.DoFn):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def process(self, file_name):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_bytes()
        yield (file_name, content)

class CreateZip(beam.DoFn):
    def process(self, file_contents):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_name, content in file_contents:
                zip_file.writestr(file_name, content)
        buffer.seek(0)
        yield buffer.getvalue()

class WriteZipToGCS(beam.DoFn):
    def __init__(self, bucket_name, zip_file_name):
        self.bucket_name = bucket_name
        self.zip_file_name = zip_file_name

    def process(self, zip_content):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket_name)
        blob = bucket.blob(self.zip_file_name)
        blob.upload_from_string(zip_content)

class DeleteFiles(beam.DoFn):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def process(self, file_names):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket_name)
        for file_name in file_names:
            blob = bucket.blob(file_name)
            if blob.exists():
                blob.delete()
                yield f'Deleted {file_name}'
            else:
                yield f'File not found: {file_name}'

def download_file_from_gcs(bucket_name, file_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        file_content = blob.download_as_bytes()
        return file_content
    except Exception as e:
        print(f"Error downloading file from GCS: {e}")
        return None

def get_base64_encoded_content(file_content):
    try:
        encoded_content = base64.b64encode(file_content).decode('utf-8')
        return encoded_content
    except Exception as e:
        print(f"Error encoding file to Base64: {e}")
        return None

def send_soap_request(soap_endpoint, xml_payload, username, password):
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
    }
    try:
        response = requests.post(
            soap_endpoint,
            data=xml_payload,
            headers=headers,
            auth=HTTPBasicAuth(username, password)
        )
        response.raise_for_status()
        print(f"Response Status Code: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return False

def create_soap_xml(base64_encoded_content, file_name, content_type):
    xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:typ="http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/types/" xmlns:erp="http://xmlns.oracle.com/apps/financials/commonModules/shared/model/erpIntegrationService/">
       <soapenv:Header/>
       <soapenv:Body>
          <typ:uploadFileToUcm>
             <typ:document>
                <erp:Content>{base64_encoded_content}</erp:Content>
                <erp:FileName>{file_name}</erp:FileName>
                <erp:ContentType>{content_type}</erp:ContentType>
                <erp:DocumentTitle>{file_name}</erp:DocumentTitle>
                <erp:DocumentAuthor>CSP_COMMON_USER1</erp:DocumentAuthor>
                <erp:DocumentSecurityGroup>FAFusionImportExport</erp:DocumentSecurityGroup>
                <erp:DocumentAccount>scm$/planningDataLoader$/import$</erp:DocumentAccount>
             </typ:document>
          </typ:uploadFileToUcm>
       </soapenv:Body>
    </soapenv:Envelope>"""
    return xml_payload

def publish_pubsub_message(topic_name, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.environ['GOOGLE_CLOUD_PROJECT'], topic_name)
    try:
        future = publisher.publish(topic_path, message.encode('utf-8'))
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"Failed to publish message: {e}")

def run_pipeline(bucket_name, prefix, zip_file_name):
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'nodal-empire-426212-c4'
    google_cloud_options.staging_location = 'gs://trailucm/staging'
    google_cloud_options.temp_location = 'gs://trailucm/temp'
    google_cloud_options.region = 'us-central1'

    with beam.Pipeline(options=options) as p:
        filenames = (
            p
            | 'CreateEmpty' >> beam.Create([None])
            | 'ListFiles' >> beam.ParDo(ListFiles(bucket_name, prefix))
        )

        file_contents = (
            filenames
            | 'DownloadFiles' >> beam.ParDo(DownloadFile(bucket_name))
        )

        zip_content = (
            file_contents
            | 'ToList' >> beam.combiners.ToList()
            | 'CreateZip' >> beam.ParDo(CreateZip())
        )

        _ = (
            zip_content
            | 'WriteToGCS' >> beam.ParDo(WriteZipToGCS(bucket_name, zip_file_name))
        )

        _ = (
            filenames
            | 'ToListForDeletion' >> beam.combiners.ToList()
            | 'DeleteFiles' >> beam.ParDo(DeleteFiles(bucket_name))
        )

def main():
    bucket_name = 'trailucm'
    prefix = ''  # Adjust as necessary
    zip_file_name = 'final.zip'

    # Run the Apache Beam pipeline to create the ZIP file
    run_pipeline(bucket_name, prefix, zip_file_name)

    # Now download the ZIP file and send it via SOAP
    soap_endpoint = 'https://elbq-dev2.fa.us2.oraclecloud.com/fscmService/ErpIntegrationService'  # Replace with your SOAP endpoint URL
    username = 'mfg_portal'  # Your username
    password = 'Oracle@123'  # Your password

    # Download the file from GCS as bytes
    file_content = download_file_from_gcs(bucket_name, zip_file_name)

    if file_content is None:
        print("Failed to download the file.")
        return

    # Get Base64 encoded content of the file
    base64_encoded_content = get_base64_encoded_content(file_content)

    if base64_encoded_content is None:
        print("Failed to encode the file.")
        return

    # Create the SOAP XML payload
    xml_payload = create_soap_xml(base64_encoded_content, zip_file_name, 'application/zip')

    # Send the SOAP request
    if send_soap_request(soap_endpoint, xml_payload, username, password):
        publish_pubsub_message('gcs_csv_zip_ucm', 'Successfully uploaded ZIP file to UCM')
    else:
        print("Failed to upload ZIP file to UCM.")

if __name__ == "__main__":
    main()
