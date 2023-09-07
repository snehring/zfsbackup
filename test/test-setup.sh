#!/usr/bin/bash
POOL=trash
zfs create ${POOL}/zfs_backup_test
zfs create ${POOL}/zfs_backup_test/source
zfs create ${POOL}/zfs_backup_test/other
zfs create ${POOL}/zfs_backup_test/destination
zfs create ${POOL}/zfs_backup_test/destination2

zfs snap ${POOL}/zfs_backup_test/source@zfsbackup-expected
zfs snap ${POOL}/zfs_backup_test/source@zfsbackup-delete
zfs snap ${POOL}/zfs_backup_test/source@zfsbackup-last-test
zfs snap ${POOL}/zfs_backup_test/other@zfsbackup-20180507-142000
zfs snap ${POOL}/zfs_backup_test/source@zfsbackup-rename
