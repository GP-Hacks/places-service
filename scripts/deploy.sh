#!/bin/bash
# scripts/deploy.sh - Simple app-only deployment
set -e

ENVIRONMENT="$1"
IMAGE="$2" 
CONTAINER_NAME="$3"
PORT="$4"
YC_OAUTH_TOKEN="$5"

PROJECT_NAME="kdt-places-service"

if [[ -z "$ENVIRONMENT" || -z "$IMAGE" || -z "$CONTAINER_NAME" || -z "$PORT" || -z "$YC_OAUTH_TOKEN" ]]; then
    echo "❌ Error: Missing required arguments"
    echo "Usage: $0 <environment> <image> <container_name> <port> <yc_oauth_token>"
    exit 1
fi

DEPLOY_PATH="/opt/apps/$CONTAINER_NAME/$ENVIRONMENT"
ENV_FILE="$DEPLOY_PATH/.env"

echo "🚀 Starting app deployment..."
echo "Environment: $ENVIRONMENT"
echo "Image: $IMAGE"
echo "Container: $CONTAINER_NAME"
echo "Port: $PORT"

# Install Yandex CLI if not present
if ! command -v yc &> /dev/null; then
    echo "📥 Installing Yandex Cloud CLI..."
    curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
    export PATH="$HOME/yandex-cloud/bin:$PATH"
    # Source the path immediately
    source "$HOME/.bashrc" 2>/dev/null || true
fi

# Make sure yc is available
if ! command -v yc &> /dev/null; then
    export PATH="$HOME/yandex-cloud/bin:$PATH"
fi

echo "🔑 Configuring Yandex Cloud CLI..."
yc config set token "$YC_OAUTH_TOKEN"
yc config set cloud-id b1grt0fvgql5big8hevj
yc config set folder-id b1gq39fmv588jocgh7to

echo "📝 Getting latest environment variables..."
sudo mkdir -p "$DEPLOY_PATH"

# Install jq if not present
if ! command -v jq &> /dev/null; then
    echo "📥 Installing jq..."
    sudo apt-get update >/dev/null 2>&1
    sudo apt-get install -y jq >/dev/null 2>&1
fi

# Get secrets and create .env file
SECRET_NAME="${PROJECT_NAME}-secrets-$ENVIRONMENT"
echo "📋 Getting secrets from: $SECRET_NAME"

yc lockbox payload get "$SECRET_NAME" --format json | \
    jq -r '.entries[] | "\(.key)=\(.text_value)"' | sudo tee "$ENV_FILE" > /dev/null

echo "🔑 Authenticating with Yandex Container Registry..."
echo "$YC_OAUTH_TOKEN" | sudo docker login \
  --username oauth \
  --password-stdin \
  cr.yandex

echo "📦 Pulling latest image: $IMAGE"
sudo docker pull "$IMAGE"

echo "🛑 Stopping old application container..."
sudo docker stop "$CONTAINER_NAME" 2>/dev/null || echo "Container was not running"
sudo docker rm "$CONTAINER_NAME" 2>/dev/null || echo "Container was not found"

# echo "🔗 Finding existing network..."
NETWORK_NAME="kdt"
# if [ -z "$NETWORK_NAME" ]; then
#     echo "⚠️  No existing network found, creating new one..."
#     NETWORK_NAME="${PROJECT_NAME}-network-$ENVIRONMENT"
#     sudo docker network create "$NETWORK_NAME"
# else
#     echo "📡 Using existing network: $NETWORK_NAME"
# fi

echo "▶️  Starting new application container..."
echo "Command: docker run -d --name $CONTAINER_NAME --env-file $ENV_FILE -p $PORT:8080 --restart unless-stopped $IMAGE"

sudo docker run -d \
  --name "$CONTAINER_NAME" \
  --network "$NETWORK_NAME" \
  --env-file "$ENV_FILE" \
  -p "$PORT:8080" \
  --restart unless-stopped \
  "$IMAGE"

echo "🏥 Waiting for application to be healthy..."
sleep 10  # Give the container time to start

# for i in {1..20}; do
#     # Check if container is running
#     if ! sudo docker ps -q --filter "name=$CONTAINER_NAME" | grep -q .; then
#         echo "❌ Container is not running!"
#         echo "📋 Container logs:"
#         sudo docker logs "$CONTAINER_NAME" --tail=20
#         exit 1
#     fi
#
#     # Check health endpoint
#     if curl -f -s "http://localhost:$PORT/health" >/dev/null 2>&1; then
#         echo "✅ Application is healthy!"
#         break
#     fi
#
#     if [ $i -eq 20 ]; then
#         echo "❌ Health check timeout"
#         echo "📋 Container logs:"
#         sudo docker logs "$CONTAINER_NAME" --tail=20
#         echo "📊 Container status:"
#         sudo docker ps --filter "name=$CONTAINER_NAME"
#         exit 1
#     fi
#
#     echo "Waiting for health check... ($i/20)"
#     sleep 15
# done

echo "🧹 Cleaning up old images..."
sudo docker image prune -f || true

echo "✅ Deployment completed successfully!"
echo "📊 Container status:"
sudo docker ps --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo "🎉 Application is ready at http://localhost:$PORT"
echo "📋 To view logs: sudo docker logs -f $CONTAINER_NAME"
echo "🔍 To check health: curl http://localhost:$PORT/health"

