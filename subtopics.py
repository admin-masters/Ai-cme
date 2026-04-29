import json
from azure.storage.queue import QueueClient

AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cmetyphoid;AccountKey=2hk2g3+VvyKJ4jqyY0QQkVI953Yf0HbLFUbhGNFjLA+Egnh7S+vgWf6JE1iDBT0OYYUEt3uKO3Hu+ASt9SxsHg==;EndpointSuffix=core.windows.net"

QUEUE_NAME = "subtopic-queue"
subtopic_ids = [
 "42AB8F44-1B86-4046-9322-D13B0C982674",
 "BF80D369-1EF0-4248-B796-47AF300B250E"
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