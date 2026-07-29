#!/bin/bash
#===============================================================================
# Architect Trading Platform - AWS Linux Production Deployment Script
# Repository: https://github.com/mrinalnilotpal/architech
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/mrinalnilotpal/architech/main/deploy_aws.sh | bash
#   OR
#   wget -qO- https://raw.githubusercontent.com/mrinalnilotpal/architech/main/deploy_aws.sh | bash
#   OR
#   ./deploy_aws.sh
#
# Supported: Amazon Linux 2023, Amazon Linux 2, Ubuntu 22.04/24.04
#===============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REPO_URL="https://github.com/mrinalnilotpal/architech.git"
INSTALL_DIR="$HOME/platform_core"
SERVICE_NAME="architect-trading"

#-------------------------------------------------------------------------------
# Helper functions
#-------------------------------------------------------------------------------
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_banner() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║     Architect Trading Platform - AWS Production Deployment       ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        VERSION=$VERSION_ID
    elif [ -f /etc/amazon-linux-release ]; then
        OS="amzn"
        VERSION="2023"
    else
        log_error "Unsupported operating system"
        exit 1
    fi
    log_info "Detected OS: $OS $VERSION"
}

#-------------------------------------------------------------------------------
# Step 1: Install system dependencies
#-------------------------------------------------------------------------------
install_dependencies() {
    log_info "Installing system dependencies..."
    
    case $OS in
        amzn|amazonlinux)
            sudo yum update -y
            sudo yum groupinstall -y "Development Tools"
            sudo yum install -y \
                git \
                cmake \
                gcc-c++ \
                make \
                openssl-devel \
                libcurl-devel \
                zlib-devel \
                wget \
                htop \
                tmux
            ;;
        ubuntu|debian)
            sudo apt update && sudo apt upgrade -y
            sudo apt install -y \
                git \
                cmake \
                g++ \
                make \
                libssl-dev \
                libcurl4-openssl-dev \
                zlib1g-dev \
                wget \
                htop \
                tmux \
                build-essential
            ;;
        rhel|centos|fedora)
            sudo dnf update -y
            sudo dnf groupinstall -y "Development Tools"
            sudo dnf install -y \
                git \
                cmake \
                gcc-c++ \
                make \
                openssl-devel \
                libcurl-devel \
                zlib-devel \
                wget \
                htop \
                tmux
            ;;
        *)
            log_error "Unsupported OS: $OS"
            exit 1
            ;;
    esac
    
    log_success "Dependencies installed"
}

#-------------------------------------------------------------------------------
# Step 2: Verify CMake version (need 3.20+)
#-------------------------------------------------------------------------------
verify_cmake() {
    log_info "Verifying CMake version..."
    
    CMAKE_VERSION=$(cmake --version | head -1 | awk '{print $3}')
    CMAKE_MAJOR=$(echo $CMAKE_VERSION | cut -d. -f1)
    CMAKE_MINOR=$(echo $CMAKE_VERSION | cut -d. -f2)
    
    if [ "$CMAKE_MAJOR" -lt 3 ] || ([ "$CMAKE_MAJOR" -eq 3 ] && [ "$CMAKE_MINOR" -lt 20 ]); then
        log_warn "CMake $CMAKE_VERSION is too old (need 3.20+). Installing newer version..."
        
        # Install CMake 3.28 from source
        cd /tmp
        wget -q https://github.com/Kitware/CMake/releases/download/v3.28.0/cmake-3.28.0-linux-x86_64.sh
        chmod +x cmake-3.28.0-linux-x86_64.sh
        sudo ./cmake-3.28.0-linux-x86_64.sh --skip-license --prefix=/usr/local
        rm cmake-3.28.0-linux-x86_64.sh
        
        # Update PATH
        export PATH=/usr/local/bin:$PATH
        echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc
        
        CMAKE_VERSION=$(cmake --version | head -1 | awk '{print $3}')
    fi
    
    log_success "CMake version: $CMAKE_VERSION"
}

#-------------------------------------------------------------------------------
# Step 3: Clone repository
#-------------------------------------------------------------------------------
clone_repository() {
    log_info "Cloning repository..."
    
    if [ -d "$INSTALL_DIR" ]; then
        log_warn "Directory $INSTALL_DIR already exists"
        read -p "Remove and re-clone? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf "$INSTALL_DIR"
        else
            log_info "Pulling latest changes instead..."
            cd "$INSTALL_DIR"
            git pull origin main
            return
        fi
    fi
    
    git clone "$REPO_URL" "$INSTALL_DIR"
    log_success "Repository cloned to $INSTALL_DIR"
}

#-------------------------------------------------------------------------------
# Step 4: Build the platform
#-------------------------------------------------------------------------------
build_platform() {
    log_info "Building platform (Release mode)..."
    
    cd "$INSTALL_DIR"
    chmod +x build.sh run_simulation.sh
    
    # Clean any previous builds
    if [ -d "bin" ]; then
        rm -rf bin
    fi
    
    # Build
    ./build.sh release
    
    # Verify build
    if [ -f "bin/ax_bin/trading_client" ]; then
        log_success "Build successful!"
        log_info "Binary location: $INSTALL_DIR/bin/ax_bin/trading_client"
    else
        log_error "Build failed - binary not found"
        exit 1
    fi
}

#-------------------------------------------------------------------------------
# Step 5: Setup configuration
#-------------------------------------------------------------------------------
setup_config() {
    log_info "Setting up configuration..."
    
    CONFIG_FILE="$INSTALL_DIR/config/default_config.json"
    
    # Create config from example if needed
    if [ ! -f "$CONFIG_FILE" ]; then
        if [ -f "$INSTALL_DIR/config/config.example.json" ]; then
            cp "$INSTALL_DIR/config/config.example.json" "$CONFIG_FILE"
            log_info "Created config from example"
        fi
    fi
    
    log_warn "IMPORTANT: Configure your API credentials!"
    echo ""
    echo "  Option 1 - Environment variables (recommended):"
    echo "    export ARCHITECT_API_KEY='your_api_key'"
    echo "    export ARCHITECT_API_SECRET='your_api_secret'"
    echo ""
    echo "  Option 2 - Edit config file:"
    echo "    nano $CONFIG_FILE"
    echo ""
}

#-------------------------------------------------------------------------------
# Step 6: Create systemd service
#-------------------------------------------------------------------------------
create_systemd_service() {
    log_info "Creating systemd service..."
    
    # Get current user
    CURRENT_USER=$(whoami)
    
    # Create service file
    sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null << EOF
[Unit]
Description=Architect Trading Platform
Documentation=https://github.com/mrinalnilotpal/architech
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$CURRENT_USER
Group=$CURRENT_USER
WorkingDirectory=$INSTALL_DIR

# Environment - set your credentials here or use EnvironmentFile
# EnvironmentFile=/home/$CURRENT_USER/.architect_env
# Environment="ARCHITECT_API_KEY=your_key_here"
# Environment="ARCHITECT_API_SECRET=your_secret_here"

# Main command - date is passed as instance parameter
ExecStart=$INSTALL_DIR/bin/ax_bin/trading_client $INSTALL_DIR/config/default_config.json %i

# Restart policy
Restart=on-failure
RestartSec=30
StartLimitBurst=3
StartLimitIntervalSec=300

# Logging
StandardOutput=append:$INSTALL_DIR/logs/service.log
StandardError=append:$INSTALL_DIR/logs/service-error.log

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$INSTALL_DIR/logs

[Install]
WantedBy=multi-user.target
EOF

    # Create logs directory
    mkdir -p "$INSTALL_DIR/logs"
    
    # Create environment file template
    cat > "$HOME/.architect_env.template" << EOF
# Architect Trading Platform - Environment Variables
# Copy this to ~/.architect_env and fill in your credentials
# Then uncomment EnvironmentFile in the systemd service

ARCHITECT_API_KEY=your_api_key_here
ARCHITECT_API_SECRET=your_api_secret_here
# ARCHITECT_SESSION_TOKEN=optional_session_token
EOF

    # Reload systemd
    sudo systemctl daemon-reload
    
    log_success "Systemd service created: ${SERVICE_NAME}"
    echo ""
    echo "  To start trading for today:"
    echo "    sudo systemctl start ${SERVICE_NAME}@\$(date +%Y%m%d)"
    echo ""
    echo "  To check status:"
    echo "    sudo systemctl status ${SERVICE_NAME}@\$(date +%Y%m%d)"
    echo ""
    echo "  To view logs:"
    echo "    journalctl -u ${SERVICE_NAME}@\$(date +%Y%m%d) -f"
    echo ""
}

#-------------------------------------------------------------------------------
# Step 7: Create convenience scripts
#-------------------------------------------------------------------------------
create_helper_scripts() {
    log_info "Creating helper scripts..."
    
    # Start trading script
    cat > "$INSTALL_DIR/start_trading.sh" << 'EOF'
#!/bin/bash
# Start trading for today (or specified date)
DATE=${1:-$(date +%Y%m%d)}
cd "$(dirname "$0")"

echo "Starting trading for date: $DATE"

# Check for credentials
if [ -z "$ARCHITECT_API_KEY" ]; then
    if [ -f "$HOME/.architect_env" ]; then
        source "$HOME/.architect_env"
    else
        echo "ERROR: ARCHITECT_API_KEY not set"
        echo "Set environment variables or create ~/.architect_env"
        exit 1
    fi
fi

# Run in foreground (use nohup or tmux for background)
./bin/ax_bin/trading_client config/default_config.json $DATE
EOF
    chmod +x "$INSTALL_DIR/start_trading.sh"
    
    # Background trading script
    cat > "$INSTALL_DIR/start_trading_bg.sh" << 'EOF'
#!/bin/bash
# Start trading in background with logging
DATE=${1:-$(date +%Y%m%d)}
cd "$(dirname "$0")"

# Load credentials
if [ -f "$HOME/.architect_env" ]; then
    source "$HOME/.architect_env"
fi

LOG_FILE="logs/${DATE}_trading.log"
mkdir -p logs

echo "Starting trading in background for date: $DATE"
echo "Log file: $LOG_FILE"

nohup ./bin/ax_bin/trading_client config/default_config.json $DATE > "$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "logs/${DATE}.pid"

echo "Started with PID: $PID"
echo "To follow logs: tail -f $LOG_FILE"
echo "To stop: kill $PID"
EOF
    chmod +x "$INSTALL_DIR/start_trading_bg.sh"
    
    # Stop trading script
    cat > "$INSTALL_DIR/stop_trading.sh" << 'EOF'
#!/bin/bash
# Stop trading gracefully
DATE=${1:-$(date +%Y%m%d)}
cd "$(dirname "$0")"

PID_FILE="logs/${DATE}.pid"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 $PID 2>/dev/null; then
        echo "Stopping trading (PID: $PID)..."
        kill -SIGINT $PID
        sleep 2
        if kill -0 $PID 2>/dev/null; then
            echo "Process still running, sending SIGTERM..."
            kill -SIGTERM $PID
        fi
        rm "$PID_FILE"
        echo "Stopped"
    else
        echo "Process not running"
        rm "$PID_FILE"
    fi
else
    echo "No PID file found. Searching for process..."
    pkill -f "trading_client.*$DATE" && echo "Stopped" || echo "No process found"
fi
EOF
    chmod +x "$INSTALL_DIR/stop_trading.sh"
    
    # Status script
    cat > "$INSTALL_DIR/status.sh" << 'EOF'
#!/bin/bash
# Check trading status
DATE=${1:-$(date +%Y%m%d)}
cd "$(dirname "$0")"

echo "=== Architect Trading Platform Status ==="
echo ""

# Check if running
PID_FILE="logs/${DATE}.pid"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 $PID 2>/dev/null; then
        echo "Status: RUNNING (PID: $PID)"
        echo ""
        echo "Process info:"
        ps -p $PID -o pid,ppid,%cpu,%mem,etime,cmd
    else
        echo "Status: STOPPED (stale PID file)"
    fi
else
    # Check systemd
    if systemctl is-active --quiet architect-trading@$DATE 2>/dev/null; then
        echo "Status: RUNNING (via systemd)"
        systemctl status architect-trading@$DATE --no-pager
    else
        echo "Status: STOPPED"
    fi
fi

echo ""
echo "Recent log entries:"
LOG_FILE="logs/${DATE}_trading.log"
if [ -f "$LOG_FILE" ]; then
    tail -10 "$LOG_FILE"
else
    echo "(no log file for today)"
fi
EOF
    chmod +x "$INSTALL_DIR/status.sh"
    
    log_success "Helper scripts created"
}

#-------------------------------------------------------------------------------
# Step 8: Print summary
#-------------------------------------------------------------------------------
print_summary() {
    echo ""
    echo -e "${GREEN}"
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║              DEPLOYMENT COMPLETE - READY FOR TRADING             ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    echo ""
    echo "Installation directory: $INSTALL_DIR"
    echo ""
    echo -e "${YELLOW}NEXT STEPS:${NC}"
    echo ""
    echo "1. Set your API credentials:"
    echo "   cp ~/.architect_env.template ~/.architect_env"
    echo "   nano ~/.architect_env   # Add your keys"
    echo "   source ~/.architect_env"
    echo ""
    echo "2. (Optional) Switch back to sandbox for paper trading:"
    echo "   nano $INSTALL_DIR/config/default_config.json"
    echo "   # Change rest_endpoint from https://gateway.architect.exchange/api"
    echo "   #                       to https://gateway.sandbox.architect.exchange/api"
    echo "   # (and ws_endpoint from wss://gateway.architect.exchange/orders/ws"
    echo "   #                  to wss://gateway.sandbox.architect.exchange/orders/ws)"
    echo ""
    echo "3. Start trading:"
    echo ""
    echo "   # Foreground (for testing):"
    echo "   cd $INSTALL_DIR && ./start_trading.sh"
    echo ""
    echo "   # Background:"
    echo "   cd $INSTALL_DIR && ./start_trading_bg.sh"
    echo ""
    echo "   # Via systemd (recommended for production):"
    echo "   sudo systemctl start architect-trading@\$(date +%Y%m%d)"
    echo ""
    echo "4. Monitor:"
    echo "   cd $INSTALL_DIR && ./status.sh"
    echo "   tail -f logs/\$(date +%Y%m%d)_trading.log"
    echo ""
    echo "5. Stop trading:"
    echo "   cd $INSTALL_DIR && ./stop_trading.sh"
    echo "   # OR: sudo systemctl stop architect-trading@\$(date +%Y%m%d)"
    echo ""
    echo -e "${BLUE}Happy Trading!${NC}"
    echo ""
}

#-------------------------------------------------------------------------------
# Main execution
#-------------------------------------------------------------------------------
main() {
    print_banner
    
    detect_os
    install_dependencies
    verify_cmake
    clone_repository
    build_platform
    setup_config
    create_systemd_service
    create_helper_scripts
    print_summary
}

# Run main function
main "$@"
