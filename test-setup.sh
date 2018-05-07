#!/usr/bin/bash

zfs create store/zfs_backup_test
zfs create store/zfs_backup_test/source
zfs create store/zfs_backup_test/other
zfs create store/zfs_backup_test/destination

zfs snap store/zfs_backup_test/source@zfsbackup-expected
zfs snap store/zfs_backup_test/source@zfsbackup-delete
zfs snap store/zfs_backup_test/source@zfsbackup-last
zfs snap store/zfs_backup_test/other@zfsbackup-20180507-1420
zfs snap store/zfs_backup_test/source@zfsbackup-rename
