#!/usr/bin/bash
POOL=archive/testing
set -x
set -e
bash integration_test-setup.sh
sudo cp -r /etc/ /${POOL}/zfs_backup_test/source
sudo cp -r /etc/ /${POOL}/zfs_backup_test/source2
python3	../zfsbackup.py --config integration_test.yml
sleep 62
sudo cp -r /usr/bin /${POOL}/zfs_backup_test/source
sudo cp -r /usr/bin /${POOL}/zfs_backup_test/source2
python3 ../zfsbackup.py --config integration_test.yml
zfs list
zfs list -t snap
bash integration_test-teardown.sh
