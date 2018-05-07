import unittest
import zfsbackup
from zfsbackup import ZFSBackupError
import subprocess
import os


class TestZFSBackup(unittest.TestCase):
    """
       Tests for the zfsbackup.py utility
    """
    # this is the base dataset we will be testing in, it will be created
    # assumed that no one will care if we destroy everything in it
    # please adjust here and in test-setup.sh/test-teardown.sh
    base_dataset = "store/zfs_backup_test"
    source_dataset = "source"
    other_dataset = "other"
    dest_dataset = "destination"

    def setUp(self):
        subprocess.run(['./test-setup.sh'],timeout=5)

    def tearDown(self):
        subprocess.run(['./test-teardown.sh'],timeout=5)

    def testLockfileCreation(self):
        fd = zfsbackup.create_lockfile("./testing")
        self.assertTrue(type(fd) is int)
        os.remove("./testing")

    
    def testLockfileCleanup(self):
        fd = zfsbackup.create_lockfile("./testing")
        zfsbackup.clean_lockfile("./testing",fd)
        self.assertFalse(os.path.exists("./testing"))

    def testCreateSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        zfsbackup.create_snapshot(dataset,'zfsbackup-unittest')

    def testListSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        desired_snap = self.base_dataset+'/'+self.source_dataset+'@zfsbackup-expected'
        snaps = zfsbackup.get_snapshots(dataset)
        self.assertTrue(desired_snap in snaps)

    def testCreateListSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        zfsbackup.create_snapshot(dataset,'zfsbackup-unittest')
        desired_snap = self.base_dataset+'/'+self.source_dataset+'@zfsbackup-unittest'
        snaps = zfsbackup.get_snapshots(dataset)
        self.assertTrue(desired_snap in snaps)

    def testCreateListDeleteSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        zfsbackup.create_snapshot(dataset,'zfsbackup-unittest')
        desired_snap = self.base_dataset+'/'+self.source_dataset+'@zfsbackup-unittest'
        snaps = zfsbackup.get_snapshots(dataset)
        self.assertTrue(desired_snap in snaps)
        zfsbackup.delete_snapshot(desired_snap)
        snaps = zfsbackup.get_snapshots(dataset)
        self.assertTrue(desired_snap not in snaps)

    def testDeleteSnapshot(self):
        snapshot = self.base_dataset+'/'+self.source_dataset+'@zfsbackup-delete'
        zfsbackup.delete_snapshot(snapshot)

    def testHasBackupLast(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        nope = self.base_dataset+'/'+self.dest_dataset
        self.assertTrue(zfsbackup.has_backuplast(dataset,'@zfsbackup-last'))
        self.assertFalse(zfsbackup.has_backuplast(nope,'@zfsbackup-last'))

    def testHasStraglers(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        nope = self.base_dataset+'/'+self.other_dataset
        self.assertFalse(zfsbackup.has_straglers(dataset))
        self.assertTrue(zfsbackup.has_straglers(nope))

    def testRenameSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        to_rename = dataset+'@zfsbackup-rename'
        rename_to = dataset+'@zfsbackup-butts'
        zfsbackup.rename_snapshot(to_rename, rename_to)
        snaps = zfsbackup.get_snapshots(dataset)
        self.assertTrue(rename_to in snaps)

    def testRenameDataset(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        rename = self.base_dataset+'/something'
        zfsbackup.rename_dataset(dataset, rename)
        desired_snap = self.base_dataset+'/something@zfsbackup-expected'
        snaps = zfsbackup.get_snapshots(rename)
        self.assertTrue(desired_snap in snaps)

    def testSendSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        snapshot = dataset+'@zfsbackup-sendtest'
        destination = self.base_dataset+'/'+self.dest_dataset
        zfsbackup.create_snapshot(dataset,'zfsbackup-sendtest')
        zfsbackup.send_snapshot(snapshot, destination)
        snaps = zfsbackup.get_snapshots(destination)
        self.assertTrue(destination+'@zfsbackup-sendtest' in snaps)

    def testSendSnapshotFail(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        snapshot = dataset+'@zfsbackup-sendtest-fail'
        destination = self.base_dataset+'/'+self.other_dataset
        try:
            zfsbackup.create_snapshot(dataset,'zfsbackup-sendtest-fail')
            zfsbackup.send_snapshot(snapshot, destination)
        except ZFSBackupError as e:
            self.assertTrue(True)
            return
        self.assertTrue(False)


    def testValidate_config(self):
        # do more than this
        c = zfsbackup.validate_config('./config_example.yml')
        self.assertTrue('log_file' in c)
        self.assertTrue('lock_file' in c)
        for ds in c.get('datasets'):
            self.assertTrue('dataset_name' in ds)
            for t in ds.get('destinations'):
                self.assertTrue(('dest' in t) and ('transport' in t))


if __name__ == '__main__':
    unittest.main()
