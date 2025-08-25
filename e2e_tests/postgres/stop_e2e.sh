#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Stopping E2E Test Infrastructure...${NC}"

# Stop transfer service
if [ -f transfer.pid ]; then
    TRANSFER_PID=$(cat transfer.pid)
    echo -e "${YELLOW}🔄 Stopping transfer service (PID: $TRANSFER_PID)...${NC}"
    if kill $TRANSFER_PID 2>/dev/null; then
        echo -e "${GREEN}✅ Transfer service stopped${NC}"
    else
        echo -e "${YELLOW}⚠️  Transfer service was not running or already stopped${NC}"
    fi
    rm -f transfer.pid
else
    echo -e "${YELLOW}⚠️  No transfer service PID file found${NC}"
fi

# Stop Docker services
echo -e "${YELLOW}🐳 Stopping Docker services...${NC}"
docker compose down -v

# Clean up log files
if [ -f transfer.log ]; then
    echo -e "${YELLOW}🧹 Cleaning up log files...${NC}"
    rm -f transfer.log
fi

echo -e "${GREEN}✅ E2E infrastructure stopped and cleaned up${NC}"