#!/bin/bash
# Combined Setup Script (01-08)
# You can divide this into separate modular scripts manually.
# Each section is labeled clearly.

################################################################################
# 01_install_packages.sh
# Run on: HEAD NODE and COMPUTE NODES
################################################################################

set -euo pipefail

echo "==> Installing dependencies (requires EPEL and CM repos)"
dnf groupinstall -y "Development Tools"
dnf install -y epel-release

dnf install -y \
    munge munge-devel \
    readline-devel openssl-devel pam-devel \
    libjwt libjwt-devel \
    mariadb-devel \
    jansson-devel \
    git cmake gcc make \
    json-c-devel http-parser-devel \
    perl-ExtUtils-MakeMaker \
    libyaml-devel jq curl wget \
    python3 python3-devel git rpm-build

# Verify installations
rpm -q munge libjwt mariadb-devel json-c-devel

################################################################################
# 02_setup_munge.sh
# Run on: HEAD NODE first, then copy munge.key to all CN nodes
################################################################################

echo "==> Setting up Munge key and directories..."
mkdir -p /etc/munge /var/log/munge /var/lib/munge
chown -R munge: /etc/munge /var/log/munge /var/lib/munge
chmod 0700 /etc/munge /var/log/munge /var/lib/munge

# Generate key only if not already present
if [[ ! -f /etc/munge/munge.key ]]; then
    /usr/sbin/create-munge-key
    echo "Munge key created."
else
    echo "Munge key already exists — skipping generation."
fi

systemctl enable --now munge
systemctl status munge --no-pager

# NOTE: After running this on the head node, copy /etc/munge/munge.key to CN nodes
#       Use scp and set permissions after copying:
#       chmod 400 /etc/munge/munge.key
#       chown munge:munge /etc/munge/munge.key

################################################################################
# 03_setup_users.sh
# Run on: HEAD NODE
################################################################################

echo "==> Creating slurm, slurmapi, and jwtuser accounts..."
useradd -m slurm || true
useradd --system --no-create-home --shell /sbin/nologin slurmapi || true
useradd -m jwtuser || true

mkdir -p /etc/slurm /var/spool/slurmd /var/log/slurm /run/slurmrestd
chown -R slurm: /etc/slurm /var/spool/slurm* /var/log/slurm /run/slurmrestd

################################################################################
# 04_mariadb_setup.sh
# Run on: HEAD NODE
################################################################################

echo "==> Setting up MariaDB for Slurm accounting..."
systemctl enable --now mariadb
mysql_secure_installation

mysql -u root <<EOF
CREATE DATABASE IF NOT EXISTS slurm_acct_db;
CREATE USER IF NOT EXISTS 'slurm'@'localhost' IDENTIFIED BY 'slurm@123';
GRANT ALL PRIVILEGES ON slurm_acct_db.* TO 'slurm'@'localhost';
FLUSH PRIVILEGES;
EOF

################################################################################
# 05_build_slurm.sh
# Run on: HEAD NODE (compile), then copy to CN nodes
################################################################################

SLURM_VERSION=24.11.3
cd /usr/local/src
wget https://download.schedmd.com/slurm/slurm-$SLURM_VERSION.tar.bz2

tar -xvjf slurm-$SLURM_VERSION.tar.bz2
cd slurm-$SLURM_VERSION

./configure --prefix=/opt/slurm --sysconfdir=/etc/slurm --with-munge --with-jwt --enable-slurmrestd
make -j$(nproc)
make install

################################################################################
# 06_config_files.sh
# Run on: HEAD NODE — config files copied from backup or use ./configs folder
################################################################################

mkdir -p /etc/slurm/jwt
openssl genpkey -algorithm RSA -out /etc/slurm/jwt/jwt.key
chown slurm:slurm /etc/slurm/jwt/jwt.key
chmod 600 /etc/slurm/jwt/jwt.key

# Provide access to API users
chown slurmapi:slurmapi /etc/slurm/jwt/jwt.key
chown jwtuser:jwtuser /etc/slurm/jwt/jwt.key

# slurm.conf, cgroup.conf, slurmdbd.conf, slurmrestd.env should be copied from configs folder
# Example:
# cp ./configs/slurm.conf /etc/slurm/
# cp ./configs/cgroup.conf /etc/slurm/
# cp ./configs/slurmrestd.env /etc/slurm/

################################################################################
# 07_systemd_services.sh
# Run on: HEAD NODE and CN as needed
################################################################################

# Copy pre-defined service files from ./configs/systemd to /etc/systemd/system/
# Example:
# cp ./configs/systemd/slurmctld.service /etc/systemd/system/
# cp ./configs/systemd/slurmdbd.service /etc/systemd/system/
# cp ./configs/systemd/slurmrestd.service /etc/systemd/system/

systemctl daemon-reexec
systemctl daemon-reload

################################################################################
# 08_setup_compute_nodes.sh
# Run on: COMPUTE NODES after building Slurm on HEAD node
################################################################################

# Assumes Slurm was built on head node at /opt/slurm
# Example copy:
# scp -r /opt/slurm slurm@n0:/opt/
# scp /etc/slurm/slurm.conf n0:/etc/slurm/
# scp /etc/munge/munge.key n0:/etc/munge/

chmod 400 /etc/munge/munge.key
chown munge:munge /etc/munge/munge.key

systemctl enable --now munge
systemctl enable --now slurmd
systemctl restart munge
systemctl restart slurmd

################################################################################
# 99_verify.sh — Run after full setup
################################################################################

# Generate token
sudo -u slurm /opt/slurm/bin/scontrol token username=jwtuser

# Export
export SLURM_JWT=your_generated_token
export SLURMRESTD_SOCKET=/run/slurmrestd/slurmrestd.sock

# Test REST API
curl --unix-socket "$SLURMRESTD_SOCKET" -H "X-SLURM-USER-TOKEN:$SLURM_JWT" http://localhost/slurm/v0.0.42/ping
curl --unix-socket "$SLURMRESTD_SOCKET" -H "X-SLURM-USER-TOKEN:$SLURM_JWT" http://localhost/slurm/v0.0.42/jobs
curl --unix-socket "$SLURMRESTD_SOCKET" -H "X-SLURM-USER-TOKEN:$SLURM_JWT" http://localhost/slurm/v0.0.42/nodes
curl --unix-socket "$SLURMRESTD_SOCKET" -H "X-SLURM-USER-TOKEN:$SLURM_JWT" http://localhost/slurmdb/v0.0.42/jobs

# Check service statuses
systemctl status munge slurmd slurmctld slurmdbd slurmrestd --no-pager
