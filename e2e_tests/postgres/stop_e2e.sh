#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Stopping E2E Test Infrastructure...${NC}"

# Stop Docker services
echo -e "${YELLOW}🐳 Stopping Docker services...${NC}"
docker compose down --volumes

echo -e "${GREEN}✅ E2E infrastructure stopped and cleaned up${NC}"