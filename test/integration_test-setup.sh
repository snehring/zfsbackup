#!/usr/bin/bash
POOL=trash
zfs create ${POOL}/zfs_backup_test
zfs create ${POOL}/zfs_backup_test/source
zfs create ${POOL}/zfs_backup_test/source2
zfs create ${POOL}/zfs_backup_test/destination
zfs create ${POOL}/zfs_backup_test/destination2_local
zfs create ${POOL}/zfs_backup_test/destination2_ssh

