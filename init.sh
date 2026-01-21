docker compose run --rm --entrypoint n8n n8n import:workflow \
  --input=/files/ingestion.json
docker compose run --rm --entrypoint n8n n8n import:workflow \
  --input=/files/scheduler.json
docker compose run --rm --entrypoint n8n n8n import:workflow \
  --input=/files/processing.json
docker compose run --rm --entrypoint n8n n8n import:workflow \
  --input=/files/generate_new_data.json
docker compose up -d

sleep 20 # Aguarda o workflow de processamento ser executado
cd ./dashboard/
sudo npm install
sudo npm run build
sudo npm install -g @angular/cli
ng serve &
