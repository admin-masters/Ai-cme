from pathlib import Path

# ---------- Azure OpenAI ----------

AZURE_OAI_ENDPOINT   = "https://azure1405.openai.azure.com/"
AZURE_OAI_KEY        = "CzrrWvXbsmYcNguU1SqBpE9HDhhbfYsbkq3UedythCYCV9zNQ4mLJQQJ99BEACHYHv6XJ3w3AAABACOGiIPm"
AZURE_OAI_DEPLOYMENT = "gpt-4o"   # deployment name
AZURE_OAI_API_VER    =  "2024-04-01-preview"

# ---------- Azure Blob ----------
BLOB_CONNECTION_STR  =  (
    "DefaultEndpointsProtocol=https;AccountName=cmetyphoid;"
    "AccountKey=2hk2g3+VvyKJ4jqyY0QQkVI953Yf0HbLFUbhGNFjLA+Egnh7S+vgWf6JE1iDBT0O"
    "YYUEt3uKO3Hu+ASt9SxsHg==;EndpointSuffix=core.windows.net"
)
CONTAINER_RAW_PDF    = "typhoidnew"
CONTAINER_CHUNKS     = "typhoidnew"
CONTAINER_META       = "typhoidnew"
CONTAINER_MANIFESTS  = "typhoidnew"

# ---------- Azure AI Search ----------
SEARCH_ENDPOINT      = "https://basic-rag-sandbox.search.windows.net"
SEARCH_ADMIN_KEY     = "tuqRZ8A374Aw3wXKSTzOY6SEu6Ra8rOyhPgFEtcLpSAzSeBOByQL"
VECTOR_INDEX_NAME    = "pubert-demo-new"
SEARCH_API_VERSION   = "2025-05-01-preview"

# ---------- Local paths ----------
LOCAL_IN_PDF         = Path("data/input_pdfs")
LOCAL_OUT            = Path("data/out")

# ---------- Embedding model ----------
HF_MODEL             = "microsoft/BiomedNLP-PubMedBERT-base-uncased-abstract"
MAX_EMB_LEN          = 768