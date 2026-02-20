#!/bin/bash
#
# Setup Automatic Pipeline Sync
# Configures cron job to run pipeline automatically
#
# Usage:
#   ./demo/setup_auto_sync.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Endymion-AI - Auto Sync Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Make sync script executable
chmod +x "$SCRIPT_DIR/auto_sync_pipeline.sh"
echo -e "${GREEN}✅${NC} Made auto_sync_pipeline.sh executable"

# Prepare cron job
CRON_COMMAND="* * * * * cd $PROJECT_ROOT && $SCRIPT_DIR/auto_sync_pipeline.sh >> /tmp/endymion_ai_cron.log 2>&1"

echo ""
echo -e "${YELLOW}Choose sync frequency:${NC}"
echo "  1) Every 1 minute  (fastest, for demos)"
echo "  2) Every 2 minutes (balanced)"
echo "  3) Every 5 minutes (production)"
echo "  4) Custom interval"
echo ""
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        CRON_SCHEDULE="* * * * *"
        INTERVAL="1 minute"
        ;;
    2)
        CRON_SCHEDULE="*/2 * * * *"
        INTERVAL="2 minutes"
        ;;
    3)
        CRON_SCHEDULE="*/5 * * * *"
        INTERVAL="5 minutes"
        ;;
    4)
        read -p "Enter cron schedule (e.g., */3 * * * * for every 3 min): " CRON_SCHEDULE
        INTERVAL="custom"
        ;;
    *)
        echo -e "${YELLOW}Invalid choice. Defaulting to every 2 minutes.${NC}"
        CRON_SCHEDULE="*/2 * * * *"
        INTERVAL="2 minutes"
        ;;
esac

CRON_COMMAND="$CRON_SCHEDULE cd $PROJECT_ROOT && $SCRIPT_DIR/auto_sync_pipeline.sh >> /tmp/endymion_ai_cron.log 2>&1"

echo ""
echo -e "${BLUE}Cron job to be installed:${NC}"
echo -e "  ${YELLOW}$CRON_COMMAND${NC}"
echo ""

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "auto_sync_pipeline.sh"; then
    echo -e "${YELLOW}⚠️  Existing auto_sync cron job found.${NC}"
    read -p "Replace it? [y/N]: " replace
    if [[ ! "$replace" =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi
    # Remove old cron job
    crontab -l 2>/dev/null | grep -v "auto_sync_pipeline.sh" | crontab -
    echo -e "${GREEN}✅${NC} Removed old cron job"
fi

# Install new cron job
(crontab -l 2>/dev/null; echo "$CRON_COMMAND") | crontab -
echo -e "${GREEN}✅${NC} Cron job installed successfully"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✅ Auto Sync Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}Configuration:${NC}"
echo -e "  Interval: ${GREEN}$INTERVAL${NC}"
echo -e "  Script:   ${GREEN}$SCRIPT_DIR/auto_sync_pipeline.sh${NC}"
echo -e "  Logs:     ${GREEN}/tmp/endymion_ai_auto_sync.log${NC}"
echo ""
echo -e "${BLUE}How it works:${NC}"
echo "  1. Cron runs the sync script every $INTERVAL"
echo "  2. Script checks for unpublished events"
echo "  3. If events exist, runs Bronze → Silver → Gold"
echo "  4. If no events, skips (fast check, no Spark startup)"
echo ""
echo -e "${BLUE}Management commands:${NC}"
echo "  View cron jobs:    crontab -l"
echo "  Remove cron job:   crontab -e  (then delete the line)"
echo "  View sync logs:    tail -f /tmp/endymion_ai_auto_sync.log"
echo "  Manual sync:       ./demo/auto_sync_pipeline.sh"
echo ""
echo -e "${BLUE}Testing:${NC}"
echo "  1. Create a cow via API: curl -X POST http://localhost:8000/api/v1/cows ..."
echo "  2. Wait $INTERVAL"
echo "  3. Check dashboard - new cow should appear in analytics"
echo ""
echo -e "${YELLOW}💡 Tip:${NC} For faster demo updates, use 1-minute interval"
echo -e "${YELLOW}💡 Tip:${NC} Watch real-time: tail -f /tmp/endymion_ai_auto_sync.log"
echo ""
