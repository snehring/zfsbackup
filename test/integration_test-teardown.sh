#!/usr/bin/bash
POOL=archive/testing
zfs destroy -r ${POOL}/zfs_backup_test
