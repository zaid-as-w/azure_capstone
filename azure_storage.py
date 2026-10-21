
import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


CONTAINER_NAME = "bronze"


def upload_file(uploaded_file):
    """Upload a Streamlit file to Azure Storage."""

    # Check environment variable first, then Streamlit Cloud secrets
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            if not account_name and "AZURE_STORAGE_ACCOUNT" in st.secrets:
                account_name = st.secrets["AZURE_STORAGE_ACCOUNT"]
            if not conn_str and "AZURE_STORAGE_CONNECTION_STRING" in st.secrets:
                conn_str = st.secrets["AZURE_STORAGE_CONNECTION_STRING"]
    except Exception:
        pass

    if not account_name and not conn_str:
        raise ValueError(
            "AZURE_STORAGE_ACCOUNT (or AZURE_STORAGE_CONNECTION_STRING) is missing. "
            "Please configure it in environment variables or Streamlit secrets."
        )

    if not uploaded_file.name.lower().endswith(".csv"):
        raise ValueError("Only CSV files are allowed.")

    filename = Path(uploaded_file.name).name
    blob_name = f"uploads/{filename}"

    service_client = None
    credential = None

    try:
        if conn_str:
            service_client = BlobServiceClient.from_connection_string(conn_str)
            account_url = service_client.primary_endpoint.rstrip("/")
        else:
            account_url = f"https://{account_name}.blob.core.windows.net"
            credential = DefaultAzureCredential()
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
        if service_client is not None:
            service_client.close()
        if credential is not None:
            credential.close()
