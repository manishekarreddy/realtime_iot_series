
docker compose down 

REM Build Docker image
docker build -t broker mqtt_proxy/.

REM Start Docker Compose
docker-compose up --build -d