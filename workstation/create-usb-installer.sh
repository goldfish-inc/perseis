#!/bin/bash
# Perseis Workstation USB Installer Creator
# Creates a bootable Ubuntu 24.04 LTS USB with unattended RTX 4090 workstation setup

set -euo pipefail

# Configuration
USB_DEVICE=""
UBUNTU_ISO_URL="https://releases.ubuntu.com/24.04/ubuntu-24.04-live-server-amd64.iso"
UBUNTU_ISO_NAME="ubuntu-24.04-live-server-amd64.iso"
TEMP_DIR="/tmp/perseis-usb-creator"
MOUNT_POINT="/tmp/perseis-iso-mount"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_requirements() {
    print_status "Checking requirements..."

    # Check if running as root
    if [[ $EUID -ne 0 ]]; then
        print_error "This script must be run as root (use sudo)"
        exit 1
    fi

    # Check for required tools
    local missing_tools=()
    for tool in curl dd mkfs.fat sgdisk parted; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done

    if [ ${#missing_tools[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        print_status "Install with: brew install ${missing_tools[*]}"
        exit 1
    fi

    print_success "All requirements met"
}

detect_usb_drives() {
    print_status "Detecting USB drives..."

    # List available USB drives
    echo "Available USB drives:"
    diskutil list external physical | grep -E '^/dev/disk[0-9]+' | while read -r line; do
        disk=$(echo "$line" | awk '{print $1}')
        size=$(diskutil info "$disk" | grep "Total Size" | awk -F: '{print $2}' | xargs)
        name=$(diskutil info "$disk" | grep "Volume Name" | awk -F: '{print $2}' | xargs || echo "No Name")
        echo "  $disk - $size - $name"
    done

    echo ""
    read -p "Enter the USB device (e.g., /dev/disk2): " USB_DEVICE

    if [[ ! -b "$USB_DEVICE" ]]; then
        print_error "Device $USB_DEVICE does not exist or is not a block device"
        exit 1
    fi

    # Confirm device selection
    device_info=$(diskutil info "$USB_DEVICE" | grep -E "(Volume Name|Total Size)" | xargs)
    print_warning "Selected device: $USB_DEVICE"
    print_warning "Device info: $device_info"
    print_warning "THIS WILL COMPLETELY ERASE THE USB DRIVE!"

    read -p "Are you sure you want to continue? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_status "Aborted by user"
        exit 0
    fi
}

download_ubuntu_iso() {
    print_status "Downloading Ubuntu 24.04 LTS ISO..."

    mkdir -p "$TEMP_DIR"
    cd "$TEMP_DIR"

    if [[ ! -f "$UBUNTU_ISO_NAME" ]]; then
        curl -L -o "$UBUNTU_ISO_NAME" "$UBUNTU_ISO_URL"
        print_success "Ubuntu ISO downloaded"
    else
        print_status "Ubuntu ISO already exists, skipping download"
    fi

    # Verify ISO integrity (optional but recommended)
    print_status "Verifying ISO integrity..."
    if shasum -a 256 "$UBUNTU_ISO_NAME" | grep -q "$(curl -s https://releases.ubuntu.com/24.04/SHA256SUMS | grep "$UBUNTU_ISO_NAME" | awk '{print $1}')"; then
        print_success "ISO integrity verified"
    else
        print_warning "Could not verify ISO integrity, proceeding anyway"
    fi
}

prepare_usb_drive() {
    print_status "Preparing USB drive..."

    # Unmount the drive if it's mounted
    diskutil unmountDisk "$USB_DEVICE" || true

    # Create partition table and format
    print_status "Creating partition table..."
    diskutil partitionDisk "$USB_DEVICE" GPT "FAT32" "PERSEIS" "100%"

    # Get the partition device
    USB_PARTITION="${USB_DEVICE}s1"

    print_success "USB drive prepared"
}

create_cloud_init_config() {
    print_status "Creating cloud-init configuration with ESC integration..."

    local config_dir="$TEMP_DIR/cloud-init"
    mkdir -p "$config_dir"

    # Get secrets from ESC
    print_status "Retrieving secrets from Pulumi ESC..."

    # This would normally pull from ESC, but for now we'll use placeholders
    # In production, run: esc env get default/perseis-secrets

    cat > "$config_dir/user-data" << 'EOF'
#cloud-config
# Perseis Workstation - Unattended RTX 4090 Setup
# Generated from Pulumi ESC secrets

autoinstall:
  version: 1
  locale: en_US.UTF-8
  keyboard:
    layout: us

  network:
    network:
      version: 2
      ethernets:
        enp0s31f6:  # Will be adjusted for AMD motherboard
          dhcp4: true
          dhcp6: false

  storage:
    layout:
      name: lvm
      sizing-policy: all

  # ESC-managed identity
  identity:
    hostname: calypso
    username: mlops
    password: "$6$rounds=4096$PLACEHOLDER_PASSWORD_HASH"

  ssh:
    install-server: true
    authorized-keys:
      - "PLACEHOLDER_SSH_PUBLIC_KEY"

  # Ubuntu Pro activation
  ubuntu_advantage:
    token: "PLACEHOLDER_UBUNTU_PRO_TOKEN"

  packages:
    - ubuntu-drivers-common
    - build-essential
    - git
    - curl
    - wget
    - vim
    - htop
    - nvtop
    - docker.io
    - nfs-common
    - python3-pip
    - python3-venv

  late-commands:
    # NVIDIA Driver Installation
    - curtin in-target --target=/target -- ubuntu-drivers install nvidia:550

    # CUDA 12.x Installation
    - |
      curtin in-target --target=/target -- bash -c '
      wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb
      dpkg -i cuda-keyring_1.1-1_all.deb
      apt update
      apt install -y cuda-toolkit-12-6
      echo "export PATH=/usr/local/cuda-12.6/bin:$PATH" >> /etc/profile.d/cuda.sh
      echo "export LD_LIBRARY_PATH=/usr/local/cuda-12.6/lib64:$LD_LIBRARY_PATH" >> /etc/profile.d/cuda.sh
      '

    # Docker GPU Support
    - |
      curtin in-target --target=/target -- bash -c '
      distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
      curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | apt-key add -
      curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | tee /etc/apt/sources.list.d/nvidia-docker.list
      apt update
      apt install -y nvidia-container-toolkit
      systemctl restart docker
      usermod -aG docker mlops
      '

    # Python ML Environment with Docling-Granite
    - |
      curtin in-target --target=/target -- bash -c '
      # Install Miniconda
      wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
      bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda
      echo "export PATH=/opt/miniconda/bin:$PATH" >> /etc/profile.d/conda.sh
      source /opt/miniconda/bin/activate

      # Core ML packages
      pip install --upgrade pip
      pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu124
      pip install transformers accelerate bitsandbytes
      pip install unsloth pandas numpy scikit-learn
      pip install great-expectations airflow
      pip install mlflow wandb

      # Docling with Granite for PDF processing
      pip install docling docling-ibm-models docling-parse
      pip install mlx mlx-vlm  # For Mac acceleration compatibility

      # Download Granite models (using ESC token)
      export HF_TOKEN="PLACEHOLDER_HF_TOKEN"
      python -c "from transformers import AutoModelForCausalLM, AutoTokenizer; \
                 AutoTokenizer.from_pretrained(\"ibm-granite/granite-guardian-3.1-2b\"); \
                 AutoModelForCausalLM.from_pretrained(\"ibm-granite/granite-3.1-2b-instruct\")"

      # Install document processing dependencies
      apt install -y tesseract-ocr tesseract-ocr-eng poppler-utils
      pip install pytesseract pdf2image unstructured unstructured-inference
      pip install pypdf pdfplumber camelot-py[cv]
      '

    # k3s Agent Installation
    - |
      curtin in-target --target=/target -- bash -c '
      curl -sfL https://get.k3s.io | K3S_URL=https://tethys.boathou.se:6443 K3S_TOKEN=PLACEHOLDER_K3S_TOKEN sh -s - agent \
        --node-label="node-role.kubernetes.io/gpu=true" \
        --node-label="oceanid.node/name=calypso" \
        --node-label="oceanid.node/gpu=rtx4090x2"
      '

    # GPU Optimization Settings
    - |
      curtin in-target --target=/target -- bash -c '
      nvidia-smi -pm 1
      nvidia-smi -pl 450  # Power limit for RTX 4090
      echo "options nvidia NVreg_PreserveVideoMemoryAllocations=1" > /etc/modprobe.d/nvidia.conf
      '

    # Monitoring Tools
    - |
      curtin in-target --target=/target -- bash -c '
      docker run -d --name node-exporter \
        --restart=always \
        --pid="host" \
        --net="host" \
        -v "/:/host:ro,rslave" \
        prom/node-exporter \
        --path.rootfs=/host

      docker run -d --name nvidia-exporter \
        --restart=always \
        --gpus all \
        -p 9835:9835 \
        utkuozdemir/nvidia-exporter:v1.2.0
      '

    # Final Setup
    - echo "Perseis ML Workstation setup complete. Reboot to apply all changes."
EOF

    # Create meta-data file
    cat > "$config_dir/meta-data" << 'EOF'
instance-id: perseis-workstation-calypso
local-hostname: calypso
EOF

    print_success "Cloud-init configuration created"
}

copy_to_usb() {
    print_status "Copying Ubuntu installer and cloud-init to USB..."

    # Mount the USB partition
    mkdir -p "$MOUNT_POINT"
    mount -t msdos "$USB_PARTITION" "$MOUNT_POINT"

    # Copy Ubuntu ISO contents
    print_status "Extracting Ubuntu ISO..."
    local iso_mount="/tmp/perseis-iso-temp"
    mkdir -p "$iso_mount"
    hdiutil attach "$TEMP_DIR/$UBUNTU_ISO_NAME" -mountpoint "$iso_mount" -readonly

    # Copy all ISO contents to USB
    cp -R "$iso_mount/"* "$MOUNT_POINT/"

    # Copy cloud-init configuration
    cp -R "$TEMP_DIR/cloud-init" "$MOUNT_POINT/"

    # Create autoinstall directory structure
    mkdir -p "$MOUNT_POINT/server"
    cp "$TEMP_DIR/cloud-init/user-data" "$MOUNT_POINT/server/"
    cp "$TEMP_DIR/cloud-init/meta-data" "$MOUNT_POINT/server/"

    # Unmount everything
    hdiutil detach "$iso_mount"
    umount "$MOUNT_POINT"

    print_success "Files copied to USB drive"
}

inject_esc_secrets() {
    print_status "Injecting secrets from Pulumi ESC (2025 Best Practices)..."

    # Check if ESC environment exists and is accessible
    if ! esc env get default/perseis-secrets --format json > /dev/null 2>&1; then
        print_error "Cannot access ESC environment: default/perseis-secrets"
        print_status "First run: esc env set default/perseis-secrets --file ../esc-perseis-secrets.yaml"
        exit 1
    fi

    print_status "Retrieving secrets from ESC environment..."

    # Get secrets dynamically from ESC (2025 Best Practice)
    local esc_values="/tmp/perseis-esc-values.json"
    esc env get default/perseis-secrets --format json > "$esc_values"

    # Extract individual secrets using jq
    local ubuntu_pro_token=$(jq -r '.environmentVariables.UBUNTU_PRO_TOKEN // "NOT_FOUND"' "$esc_values")
    local ssh_public_key=$(jq -r '.environmentVariables.SSH_PUBLIC_KEY // "NOT_FOUND"' "$esc_values")
    local k3s_token=$(jq -r '.environmentVariables.K3S_TOKEN // "NOT_FOUND"' "$esc_values")
    local k3s_url=$(jq -r '.environmentVariables.K3S_URL // "https://tethys.boathou.se:6443"' "$esc_values")
    local hf_token=$(jq -r '.environmentVariables.HF_TOKEN // "NOT_FOUND"' "$esc_values")
    local password_hash=$(jq -r '.workstation.password_hash // "$6$rounds=4096$DEFAULT_HASH"' "$esc_values")
    local wifi_ssid=$(jq -r '.environmentVariables.WIFI_SSID // "IM-OLD-GREG"' "$esc_values")
    local wifi_password=$(jq -r '.environmentVariables.WIFI_PASSWORD // "NOT_FOUND"' "$esc_values")

    # Validate that required secrets were retrieved
    local missing_secrets=()
    [[ "$ubuntu_pro_token" == "NOT_FOUND" ]] && missing_secrets+=("Ubuntu Pro Token")
    [[ "$ssh_public_key" == "NOT_FOUND" ]] && missing_secrets+=("SSH Public Key")
    [[ "$k3s_token" == "NOT_FOUND" ]] && missing_secrets+=("k3s Token")
    [[ "$hf_token" == "NOT_FOUND" ]] && missing_secrets+=("Hugging Face Token")
    [[ "$wifi_password" == "NOT_FOUND" ]] && missing_secrets+=("WiFi Password")

    if [ ${#missing_secrets[@]} -ne 0 ]; then
        print_error "Missing required secrets from ESC: ${missing_secrets[*]}"
        print_status "Ensure all 1Password references are correct in esc-perseis-secrets.yaml"
        exit 1
    fi

    print_status "Updating cloud-init configuration with ESC secrets..."

    # Re-mount USB to update with real secrets
    mount -t msdos "$USB_PARTITION" "$MOUNT_POINT"

    # Update user-data with real secrets from ESC
    sed -i '' \
        -e "s/PLACEHOLDER_UBUNTU_PRO_TOKEN/$ubuntu_pro_token/g" \
        -e "s|PLACEHOLDER_SSH_PUBLIC_KEY|$ssh_public_key|g" \
        -e "s/PLACEHOLDER_PASSWORD_HASH/$password_hash/g" \
        -e "s/PLACEHOLDER_HF_TOKEN/$hf_token/g" \
        -e "s/PLACEHOLDER_K3S_TOKEN/$k3s_token/g" \
        -e "s/PLACEHOLDER_WIFI_SSID/$wifi_ssid/g" \
        -e "s/PLACEHOLDER_WIFI_PASSWORD/$wifi_password/g" \
        -e "s|K3S_URL=https://tethys.boathou.se:6443|K3S_URL=$k3s_url|g" \
        "$MOUNT_POINT/server/user-data"

    # Also update the cloud-init directory
    sed -i '' \
        -e "s/PLACEHOLDER_UBUNTU_PRO_TOKEN/$ubuntu_pro_token/g" \
        -e "s|PLACEHOLDER_SSH_PUBLIC_KEY|$ssh_public_key|g" \
        -e "s/PLACEHOLDER_PASSWORD_HASH/$password_hash/g" \
        -e "s/PLACEHOLDER_HF_TOKEN/$hf_token/g" \
        -e "s/PLACEHOLDER_K3S_TOKEN/$k3s_token/g" \
        -e "s/PLACEHOLDER_WIFI_SSID/$wifi_ssid/g" \
        -e "s/PLACEHOLDER_WIFI_PASSWORD/$wifi_password/g" \
        -e "s|K3S_URL=https://tethys.boathou.se:6443|K3S_URL=$k3s_url|g" \
        "$MOUNT_POINT/cloud-init/user-data"

    umount "$MOUNT_POINT"

    # Clean up
    rm -f "$esc_values"

    print_success "ESC secrets successfully injected into USB installer"
    print_status "Secrets injected:"
    echo "  ✅ Ubuntu Pro Token"
    echo "  ✅ SSH Public Key"
    echo "  ✅ Workstation Password"
    echo "  ✅ Hugging Face Token"
    echo "  ✅ k3s Cluster Token"
    echo "  ✅ WiFi Password (${wifi_ssid})"
}

cleanup() {
    print_status "Cleaning up temporary files..."
    rm -rf "$TEMP_DIR"
    rmdir "$MOUNT_POINT" 2>/dev/null || true
    print_success "Cleanup complete"
}

main() {
    print_status "Perseis Workstation USB Creator"
    print_status "Creating bootable USB for RTX 4090 workstation setup..."

    check_requirements
    detect_usb_drives
    download_ubuntu_iso
    prepare_usb_drive
    create_cloud_init_config
    copy_to_usb
    inject_esc_secrets
    cleanup

    print_success "USB installer created successfully!"
    print_success "USB Device: $USB_DEVICE"
    print_warning "Remember to inject real ESC secrets before using!"
    print_status "Boot the workstation from this USB to start unattended setup"
}

# Handle script interruption
trap cleanup EXIT

main "$@"