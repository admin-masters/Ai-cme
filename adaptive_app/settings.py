"""Central place to load env vars & direct configuration."""
import os
from functools import lru_cache
from typing import Optional

# Set this to False to skip Key Vault entirely and use direct values
USE_KEY_VAULT = False

# If you want to use Key Vault, set this to True and ensure proper authentication
KV_URL = os.getenv("KEYVAULT_URL") or "https://cme0207.vault.azure.net/"

DATABASE_URL =  "mssql+pyodbc://new_root:japl%40bJBYV77@20.171.24.17/CME2?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes"
@lru_cache
def get_secret(name: str) -> Optional[str]:
    # First, try environment variables
    env_value = os.getenv(name)
    if env_value:
        return env_value
    
    # Skip Key Vault if disabled
    if not USE_KEY_VAULT:
        return None
    
    # Only try Key Vault if enabled and we have a URL
    if not KV_URL:
        return None
    
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
        
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=KV_URL, credential=credential)
        # Convert underscore to hyphen for Key Vault naming convention
        kv_name = name.replace("_", "-")
        return client.get_secret(kv_name).value
    except Exception as e:
        # Log the error if needed, but don't crash
        print(f"Warning: Could not retrieve secret '{name}' from Key Vault: {e}")
        return None


class Settings:
    # SQLAlchemy URL format for SQL Server (converted from ODBC connection string)
    sql_connection: str = (
        get_secret("SQL_CONNECTION_STRING") or 
        "mssql+aioodbc://localhost\\MSSQLSERVER03/CME?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&Encrypt=no&TrustServerCertificate=yes"
    )
    
    openai_api_key: str = (
        get_secret("AZURE_OPENAI_KEY") or 
        "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"
    )
    
    openai_endpoint: str = (
        get_secret("AZURE_OPENAI_ENDPOINT") or 
        "https://azure1405.openai.azure.com/"
    )
    
    openai_deployment: str = "gpt-4o-2025-06-01"  # deployment name
    launch_signing_secret: str = get_secret("LAUNCH_SIGNING_SECRET") or "P7xi7yMI1h8BZ6ahAMwUBX4m"


settings = Settings()