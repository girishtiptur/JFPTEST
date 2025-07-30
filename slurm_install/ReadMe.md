# README: Slurm + slurmrestd Setup Guide

This guide explains how to manually install and configure a full Slurm cluster along with the `slurmrestd` REST API interface.

> This guide assumes you are **not using Git**, and instead have received:
>
> * A folder with **modular setup scripts** (01–08)
> * A `configs/` directory containing preconfigured files from a working cluster

---

## Prerequisites

* OS: RHEL 8 / Rocky Linux / AlmaLinux 8 (or compatible)
* Root/sudo access on all nodes
* Internet access or access to internal package mirrors
* CM repository available for Slurm-related packages
* Passwordless SSH from Head Node to Compute Nodes

---

## Architecture

| Role            | Description                                    |
| --------------- | ---------------------------------------------- |
| Head Node       | Runs `slurmctld`, `slurmdbd`, and `slurmrestd` |
| Compute Node(s) | Runs `slurmd` and shares configs from head     |

---

## Directory Structure

```
slurm-setup/
├── 01_install_packages.sh
├── 02_setup_munge.sh
├── 03_setup_users.sh
├── 04_mariadb_setup.sh
├── 05_build_slurm.sh
├── 06_config_files.sh
├── 07_systemd_services.sh
├── 08_setup_compute_nodes.sh
├── 99_verify.sh
└── configs/
    ├── slurm.conf
    ├── cgroup.conf
    ├── slurmdbd.conf
    ├── slurmrestd.env
    ├── jwt.key (optional - securely stored)
    └── systemd/
        ├── slurmctld.service
        ├── slurmd.service
        ├── slurmdbd.service
        └── slurmrestd.service
```

---

## Installation Steps

Run each script in order on the specified nodes. Each script prints a summary of what it does.

1. Install Required Packages

```bash
bash 01_install_packages.sh
```

Run on head node and all compute nodes.

2. Setup Munge (on Head Node)

```bash
bash 02_setup_munge.sh
```

Afterwards, manually copy `/etc/munge/munge.key` to all compute nodes and run permissions fix.

3. Setup Slurm Users

```bash
bash 03_setup_users.sh
```

4. Setup MariaDB

```bash
bash 04_mariadb_setup.sh
```

Follow the prompts to secure MariaDB and create the Slurm accounting database.

5. Build and Install Slurm

```bash
bash 05_build_slurm.sh
```

Builds Slurm with `--enable-slurmrestd` and JWT. Binary path will be `/opt/slurm`.

6. Install Configuration Files

```bash
bash 06_config_files.sh
```

Copies files from the `configs/` directory into `/etc/slurm/`. You may review/edit as needed.

7. Setup Systemd Services

```bash
bash 07_systemd_services.sh
```

Ensure all `.service` files in `configs/systemd/` are in `/etc/systemd/system/`, then run this script.

8. Setup Compute Nodes

```bash
bash 08_setup_compute_nodes.sh
```

Run this manually on each compute node after copying Slurm install and config files from the head node.

---

## Final Verification (Run on Head Node)

```bash
bash 99_verify.sh
```

This will:

* Generate a JWT token
* Export it into the shell
* Use `curl` to ping `/ping`, `/jobs`, `/nodes`, etc. on both slurm and slurmdb endpoints

Make sure:

* `slurmctld`, `slurmdbd`, and `slurmrestd` are running
* `curl` calls return valid responses with 200 OK

---

## Manual Copy Checklist

After head node setup:

* [ ] `scp /opt/slurm` → all CN nodes
* [ ] `scp /etc/slurm/slurm.conf` → all CN nodes
* [ ] `scp /etc/munge/munge.key` → all CN nodes
* [ ] `scp /opt/slurm/lib/slurm/cgroup_v2.so` → all CN nodes
* [ ] Restart `munge` and `slurmd` on all CNs

---

## Troubleshooting

* Run `journalctl -xeu <service>` to see logs
* Check permissions on `/etc/slurm/jwt/jwt.key`
* Use `scontrol token username=jwtuser` to re-generate JWT
* Run `scontrol ping` to verify node communication
* Validate RESTD socket exists: `/run/slurmrestd/slurmrestd.sock`

---

## References

* [Slurm REST API](https://slurm.schedmd.com/slurmrestd.html)
* [Slurm Configurator](https://slurm.schedmd.com/configurator.easy.html)
* Internal working setup (backup folder provided)

---

Let your admin know if you run into service start issues or REST API timeouts.

Happy clustering!
