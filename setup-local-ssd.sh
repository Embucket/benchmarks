#!/usr/bin/env bash
set -euo pipefail

# Source shared environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

RAID_DEV="/dev/md0"
FS_TYPE="xfs"
RAID_CHUNK_KB="1024"   # good for large sequential scans

# Figure out who should own the mount (the invoking sudo user if present)
OWNER_USER="${SUDO_USER:-$(whoami)}"
OWNER_GROUP="$(id -gn "${OWNER_USER}")"

echo ">>> Detecting instance-store NVMe devices (excluding root/EBS and any mounted devices)..."

# List bare NVMe *disk* devices that are Instance Store and not mounted
mapfile -t INST_DISKS < <(
  lsblk -ndo NAME,TYPE,MODEL,MOUNTPOINT | awk '
    $2=="disk" && $3 ~ /Amazon EC2 NVMe Instance Storage/ && ($4=="" || $4=="-") { print $1 }'
)

if (( ${#INST_DISKS[@]} == 0 )); then
  echo "No instance-store NVMe disks detected. Nothing to do."
  exit 0
fi

echo "Found instance-store devices:"
for d in "${INST_DISKS[@]}"; do echo "  /dev/${d}"; done
echo

sudo mkdir -p "${MOUNT_POINT}"

ensure_fs() {
  local dev="$1"
  local fstype
  fstype="$(blkid -o value -s TYPE "${dev}" || true)"
  if [[ -z "${fstype}" ]]; then
    echo ">>> Creating ${FS_TYPE} filesystem on ${dev} ..."
    sudo mkfs."${FS_TYPE}" -f "${dev}"
  else
    echo ">>> ${dev} already has filesystem type: ${fstype} (leaving as-is)"
  fi
}

persist_fstab() {
  local dev="$1"
  local mp="$2"
  local uuid
  uuid="$(blkid -o value -s UUID "${dev}")"
  local entry="UUID=${uuid} ${mp} ${FS_TYPE} defaults,noatime 0 0"

  if ! grep -qs "UUID=${uuid} " /etc/fstab; then
    echo ">>> Adding to /etc/fstab:"
    echo "    ${entry}"
    echo "${entry}" | sudo tee -a /etc/fstab >/dev/null
  else
    echo ">>> /etc/fstab already contains UUID=${uuid} entry (skipping)"
  fi
}

mount_mp() {
  local dev="$1"
  local mp="$2"
  if mountpoint -q "${mp}"; then
    echo ">>> ${mp} already mounted (skipping mount)"
  else
    echo ">>> Mounting ${dev} -> ${mp}"
    sudo mount "${dev}" "${mp}"
  fi
}

record_mdconfig() {
  echo ">>> Recording mdadm array config & updating initramfs"
  sudo mdadm --detail --scan | sudo tee -a /etc/mdadm/mdadm.conf >/dev/null
  sudo update-initramfs -u || true
}

# If we have 2+ instance-store disks, create/assemble RAID0; else use a single disk
if (( ${#INST_DISKS[@]} >= 2 )); then
  echo ">>> Using RAID0 over ${#INST_DISKS[@]} local SSDs"

  # Build device list like: /dev/nvme0n1 /dev/nvme2n1 ...
  RAID_MEMBERS=()
  for n in "${INST_DISKS[@]}"; do RAID_MEMBERS+=( "/dev/${n}" ); done

  # If /dev/md0 exists/active, try to assemble, else (re)create
  if grep -qs "^md0 :" /proc/mdstat; then
    echo ">>> md0 appears active; attempting assemble (or already assembled)"
    sudo mdadm --assemble --scan || true
  elif [[ -b "${RAID_DEV}" ]]; then
    echo ">>> ${RAID_DEV} exists; attempting assemble"
    sudo mdadm --assemble "${RAID_DEV}" "${RAID_MEMBERS[@]}" || true
  else
    echo ">>> Creating RAID0 array at ${RAID_DEV}"
    sudo mdadm --create "${RAID_DEV}" --level=0 --raid-devices="${#RAID_MEMBERS[@]}" \
      --chunk="${RAID_CHUNK_KB}" "${RAID_MEMBERS[@]}"
  fi

  echo
  cat /proc/mdstat || true
  sudo mdadm --detail "${RAID_DEV}" || true
  echo

  ensure_fs "${RAID_DEV}"
  mount_mp "${RAID_DEV}" "${MOUNT_POINT}"
  persist_fstab "${RAID_DEV}" "${MOUNT_POINT}"
  record_mdconfig

else
  # Single local NVMe
  DEV="/dev/${INST_DISKS[0]}"
  echo ">>> Only one instance-store disk detected: ${DEV}"
  ensure_fs "${DEV}"
  mount_mp "${DEV}" "${MOUNT_POINT}"
  persist_fstab "${DEV}" "${MOUNT_POINT}"
fi

echo ">>> Setting ownership on ${MOUNT_POINT} to ${OWNER_USER}:${OWNER_GROUP}"
sudo chown -R "${OWNER_USER}:${OWNER_GROUP}" "${MOUNT_POINT}"

echo
echo ">>> Verification:"
df -hT "${MOUNT_POINT}"
echo
lsblk -o NAME,MODEL,SIZE,MOUNTPOINT | sed 's/^/  /'
echo
echo "Done. Put your data under ${MOUNT_POINT}/..."
