import json
from azure.storage.queue import QueueClient

AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=cmetyphoid;AccountKey=2hk2g3+VvyKJ4jqyY0QQkVI953Yf0HbLFUbhGNFjLA+Egnh7S+vgWf6JE1iDBT0OYYUEt3uKO3Hu+ASt9SxsHg==;EndpointSuffix=core.windows.net"

QUEUE_NAME = "plan-queue"

topic_ids = [
"b04a5124-b2c8-4169-8c6f-e956a40948c0",

]

queue = QueueClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING,
    QUEUE_NAME
)

for sid in topic_ids:
    message = {"topic_id": sid}
    queue.send_message(json.dumps(message))
    print(f"Queued: {sid}")

print(f"\n✅ {len(topic_ids)} subtopics pushed to {QUEUE_NAME}")