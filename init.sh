docker compose run --rm --entrypoint n8n n8n import:workflow \
  --input=/files/ingestion.json
docker compose run --rm --entrypoint n8n n8n import:workflow \
  --input=/files/scheduler.json

docker compose up -d
