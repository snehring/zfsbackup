#!/usr/bin/bash

zfs create store/zfs_backup_test
zfs create store/zfs_backup_test/source
zfs create store/zfs_backup_test/source2
zfs create store/zfs_backup_test/destination
zfs create store/zfs_backup_test/destination2_local
zfs create store/zfs_backup_test/destination2_ssh

