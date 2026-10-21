
import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


CONTAINER_NAME = "bronze"


def upload_file(uploaded_file):
    """Upload a Streamlit file to Azure Storage."""

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT")

    if not account_name:
        raise ValueError(
            "AZURE_STORAGE_ACCOUNT environment variable is missing."
        )

    if not uploaded_file.name.lower().endswith(".csv"):
        raise ValueError("Only CSV files are allowed.")

    # Use only the filename, not a user-supplied directory path.
    filename = Path(uploaded_file.name).name
    blob_name = f"uploads/{filename}"

    account_url = (
        f"https://{account_name}.blob.core.windows.net"
    )

    credential = DefaultAzureCredential()

    try:
        service_client = BlobServiceClient(
            account_url=account_url,
            credential=credential
        )

        blob_client = service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_name
        )

        blob_client.upload_blob(
            uploaded_file.getvalue(),
            overwrite=True
        )

        return {
            "container": CONTAINER_NAME,
            "blob_name": blob_name,
            "url": f"{account_url}/{CONTAINER_NAME}/{blob_name}"
        }

    finally:
        if "service_client" in locals():
            service_client.close()

        credential.close()
