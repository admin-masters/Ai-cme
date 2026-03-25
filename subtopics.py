import json
from azure.storage.queue import QueueClient

AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cmetyphoid;AccountKey=2hk2g3+VvyKJ4jqyY0QQkVI953Yf0HbLFUbhGNFjLA+Egnh7S+vgWf6JE1iDBT0OYYUEt3uKO3Hu+ASt9SxsHg==;EndpointSuffix=core.windows.net"

QUEUE_NAME = "subtopic-queue"
subtopic_ids = [
"4C588C16-94A6-43CD-B7CB-5AF209F20C0B",
"4249187F-0D55-4C28-8037-69FF370D3739",
"0A842D98-58CB-4254-9CE2-A1B8C2B8FB47",
"CA85AD27-EFC3-4626-86E6-8E8ED398C954",

]
queue = QueueClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING,
    QUEUE_NAME
)

for sid in subtopic_ids:
    message = {"subtopic_id": sid}
    queue.send_message(json.dumps(message))
    print(f"Queued: {sid}")

print(f"\n✅ {len(subtopic_ids)} subtopics pushed to {QUEUE_NAME}")