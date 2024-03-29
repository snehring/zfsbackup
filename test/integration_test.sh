#!/usr/bin/bash
POOL=trash
set -x
set -e
bash integration_test-setup.sh
cp -r /etc/ /${POOL}/zfs_backup_test/source
cp -r /etc/ /${POOL}/zfs_backup_test/source2
python3	../zfsbackup.py --config integration_test.yml
sleep 62
cp -r /usr/bin /${POOL}/zfs_backup_test/source
cp -r /usr/bin /${POOL}/zfs_backup_test/source2
python3 ../zfsbackup.py --config integration_test.yml
zfs list
zfs list -t snap
bash integration_test-teardown.sh
