#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting E2E Test Infrastructure...${NC}"

# Phase 1: Start Docker services
echo -e "${YELLOW}📦 Phase 1: Starting Docker services...${NC}"
docker-compose up -d

# Wait for services to be ready
echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
sleep 5

# Check Postgres
echo -e "${YELLOW}🔍 Checking Postgres...${NC}"
for i in {1..30}; do
    if docker exec postgres psql -U postgres -d destination_e2e -c "SELECT 1;" >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Postgres is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}❌ Postgres failed to start${NC}"
        exit 1
    fi
    sleep 1
done

# Check Kafka
echo -e "${YELLOW}🔍 Checking Kafka...${NC}"
for i in {1..30}; do
    if docker exec postgres-kafka-1 /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Kafka is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}❌ Kafka failed to start${NC}"
        exit 1
    fi
    sleep 1
done

# Phase 2: Publish test data
echo -e "${YELLOW}📤 Phase 2: Publishing test data to Kafka...${NC}"
go run producer.go
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to publish test data${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Test data published successfully${NC}"

# Phase 3: Start transfer service
echo -e "${YELLOW}⚡ Phase 3: Starting transfer service...${NC}"
cd ../../
nohup go run main.go -c e2e_tests/postgres/config/e2e.yaml -v > e2e_tests/postgres/transfer.log 2>&1 &
TRANSFER_PID=$!
echo $TRANSFER_PID > e2e_tests/postgres/transfer.pid

echo -e "${GREEN}🎉 Transfer service started with PID: $TRANSFER_PID${NC}"
echo -e "${BLUE}📋 Logs are being written to: e2e_tests/postgres/transfer.log${NC}"
echo -e "${YELLOW}⏳ Waiting for service to initialize...${NC}"
sleep 1

echo -e "${GREEN}✅ E2E infrastructure is ready!${NC}"
echo -e "${BLUE}📊 Run './test_e2e.sh' to validate data transfer${NC}"
echo -e "${BLUE}🛑 Run './stop_e2e.sh' to clean up when done${NC}"