import json
from azure.storage.queue import QueueClient

AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cmetyphoid;AccountKey=2hk2g3+VvyKJ4jqyY0QQkVI953Yf0HbLFUbhGNFjLA+Egnh7S+vgWf6JE1iDBT0OYYUEt3uKO3Hu+ASt9SxsHg==;EndpointSuffix=core.windows.net"

QUEUE_NAME = "subtopic-queue"
subtopic_ids = [
 "20B332F6-A0C4-4A97-9A79-B76D2E0E227F",
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