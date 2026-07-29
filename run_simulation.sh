#!/bin/bash
#
# Run simulation with API keys from environment
#
# Usage:
#   export ARCHITECT_API_KEY="your_api_key"
#   export ARCHITECT_API_SECRET="your_api_secret"
#   export ARCHITECT_SESSION_TOKEN="your_session_token"
#   ./run_simulation.sh [config_file] [date]
#
# Or inline:
#   ARCHITECT_API_KEY=xxx ARCHITECT_API_SECRET=yyy ./run_simulation.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${1:-config/my_config.json}"
SIM_DATE="${2:-$(date +%Y%m%d)}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}  Architect Platform - Simulation Runner${NC}"
echo -e "${BLUE}==========================================${NC}"

# Check for required environment variables
if [[ -z "${ARCHITECT_API_KEY}" ]]; then
    echo -e "${RED}ERROR: ARCHITECT_API_KEY not set${NC}"
    echo ""
    echo "Please set your API credentials:"
    echo "  export ARCHITECT_API_KEY='your_api_key'"
    echo "  export ARCHITECT_API_SECRET='your_api_secret'"
    echo "  export ARCHITECT_SESSION_TOKEN='your_session_token'"
    exit 1
fi

# Check if binary exists
if [[ ! -f "${SCRIPT_DIR}/bin/ax_bin/trading_client" ]]; then
    echo -e "${RED}ERROR: Binary not found. Run ./build.sh first${NC}"
    exit 1
fi

# Check if config exists
if [[ ! -f "${SCRIPT_DIR}/${CONFIG_FILE}" ]]; then
    echo -e "${RED}ERROR: Config file not found: ${CONFIG_FILE}${NC}"
    exit 1
fi

# Create temp config with injected API keys
TEMP_CONFIG=$(mktemp)
trap "rm -f ${TEMP_CONFIG}" EXIT

# Use jq if available, otherwise use sed
if command -v jq &> /dev/null; then
    jq --arg key "$ARCHITECT_API_KEY" \
       --arg secret "$ARCHITECT_API_SECRET" \
       --arg token "${ARCHITECT_SESSION_TOKEN:-}" \
       '.api.api_key = $key | .api.api_secret = $secret | .api.session_token = $token' \
       "${SCRIPT_DIR}/${CONFIG_FILE}" > "${TEMP_CONFIG}"
else
    # Fallback to sed (less robust but works)
    sed -e "s/YOUR_API_KEY_HERE/${ARCHITECT_API_KEY}/g" \
        -e "s/YOUR_API_SECRET_HERE/${ARCHITECT_API_SECRET}/g" \
        -e "s/YOUR_SESSION_TOKEN_HERE/${ARCHITECT_SESSION_TOKEN:-}/g" \
        "${SCRIPT_DIR}/${CONFIG_FILE}" > "${TEMP_CONFIG}"
fi

echo -e "${GREEN}Config:${NC} ${CONFIG_FILE}"
echo -e "${GREEN}Date:${NC} ${SIM_DATE}"
echo -e "${GREEN}API Key:${NC} ${ARCHITECT_API_KEY:0:8}..."
echo ""

# Create logs directory
mkdir -p "${SCRIPT_DIR}/logs/${SIM_DATE}"

echo -e "${YELLOW}Starting simulation...${NC}"
echo ""

# Run the trading client
"${SCRIPT_DIR}/bin/ax_bin/trading_client" "${TEMP_CONFIG}" "${SIM_DATE}"

echo ""
echo -e "${GREEN}Simulation complete!${NC}"
echo -e "Logs: ${SCRIPT_DIR}/logs/${SIM_DATE}/"
