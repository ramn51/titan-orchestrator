#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

COMMAND=$1

if [ "$COMMAND" == "logs" ]; then
    echo -e "${CYAN}[INFO] Tailing all Titan logs... (Press Ctrl+C to stop watching)${NC}\n"
    tail -f /tmp/titan-store.log /tmp/titan-master.log /tmp/titan-worker.log /tmp/titan-dashboard.log
    exit 0
fi

if [ "$COMMAND" != "up" ]; then
    echo -e "${RED}Usage: ./titan-dev [up | logs]${NC}"
    exit 1
fi

echo -e "${BLUE}[INFO] Booting Titan Development Environment...${NC}\n"

JAR_FILE="target/titan-orchestrator-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${YELLOW}[BUILD] Engine JAR not found. Building with Maven...${NC}"
    mvn clean package -DskipTests > /tmp/titan-build.log 2>&1
    if [ $? -ne 0 ]; then
        echo -e "${RED}[ERROR] Maven build failed. Check /tmp/titan-build.log${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}[OK] Engine JAR found.${NC}"
fi

echo -e "${YELLOW}[SETUP] Verifying Python SDK...${NC}"
pip install -e . --quiet
echo -e "${GREEN}[OK] Python SDK ready.${NC}"

echo -e "${YELLOW}[START] Starting TitanStore (Port 6379)...${NC}"
java -jar perm_files/TitanStore.jar > /tmp/titan-store.log 2>&1 &
STORE_PID=$!
sleep 1 

echo -e "${YELLOW}[START] Starting Titan Master (Port 9090)...${NC}"
java -cp $JAR_FILE titan.TitanMaster > /tmp/titan-master.log 2>&1 &
MASTER_PID=$!
sleep 2 

echo -e "${YELLOW}[START] Starting General Worker (Port 8080)...${NC}"
java -cp $JAR_FILE titan.TitanWorker > /tmp/titan-worker.log 2>&1 &
WORKER_PID=$!

if python3 -c "import flask" &> /dev/null; then
    echo -e "${YELLOW}[START] Starting UI Dashboard (Port 5000)...${NC}"
    python3 ./perm_files/server_dashboard.py > /tmp/titan-dashboard.log 2>&1 &
    UI_PID=$!
    DASH_MSG="${GREEN}[UI] Dashboard:      http://localhost:5000${NC}"
else
    UI_PID=""
    DASH_MSG="${YELLOW}[WARN] Flask not found. Dashboard skipped.${NC}"
fi

trap cleanup INT TERM
function cleanup() {
    echo -e "\n${RED}[STOP] Caught exit signal! Commencing graceful teardown...${NC}"
    
    kill $STORE_PID 2>/dev/null
    kill $MASTER_PID 2>/dev/null
    kill $WORKER_PID 2>/dev/null
    if [ ! -z "$UI_PID" ]; then
        kill $UI_PID 2>/dev/null
    fi
    
    echo -e "${GREEN}[DONE] All Titan processes terminated. Ports are clean.${NC}"
    exit 0
}

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}               TITAN CLUSTER IS LIVE                ${NC}"
echo -e "${GREEN}====================================================${NC}"
echo -e "[+] Master Node:     localhost:9090 (PID: $MASTER_PID)"
echo -e "[+] Worker Node:     localhost:8080 (PID: $WORKER_PID)"
echo -e "[+] TitanStore:      localhost:6379 (PID: $STORE_PID)"
echo -e "$DASH_MSG"
echo -e "[+] View Logs:       Open a new terminal and run: ${CYAN}./titan-dev logs${NC}"
echo -e "${GREEN}====================================================${NC}"
echo -e "${YELLOW}[INFO] Cluster is running. Press [Ctrl+C] right here to safely shut down everything.${NC}"

wait