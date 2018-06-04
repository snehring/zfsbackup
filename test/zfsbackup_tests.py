import unittest
import zfsbackup
from zfsbackup import ZFSBackupError
import subprocess
import os
import time

def generateLargeFile(path,size=1):
    """
    Generate a large file at a given path of size GB.
    Default to 1 GB.
    """
    with open(path, mode='wb') as f:
        f.write(bytearray(os.urandom(1024**3*size)))
    subprocess.run(['sync'])


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
        self.assertTrue(type(fd) is int and os.path.exists("./testing"))
        os.remove("./testing")

    
    def testLockfileCleanup(self):
        fd = zfsbackup.create_lockfile("./testing")
        zfsbackup.clean_lockfile("./testing",fd)
        self.assertFalse(os.path.exists("./testing"))

    def testCreateSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        snap = dataset+"@zfsbackup-unittest"
        zfsbackup.create_snapshot(dataset,'zfsbackup-unittest')
        snaps = zfsbackup.get_snapshots(dataset)
        self.assertTrue(snap in snaps)

    def testCreateTimestampSnapshot(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        snaps = zfsbackup.get_snapshots(dataset)
        self.assertTrue(dataset+snap in snaps)

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
        self.assertTrue(zfsbackup.has_backuplast(dataset,'@zfsbackup-last-test'))
        self.assertFalse(zfsbackup.has_backuplast(nope,'@zfsbackup-last-test'))

    def testHasStraglers(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        nope = self.base_dataset+'/'+self.other_dataset
        self.assertFalse(zfsbackup.has_stragglers(dataset))
        self.assertTrue(zfsbackup.has_stragglers(nope))

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
            snaps = zfsbackup.get_snapshots(destination)
            self.assertFalse(snapshot in snaps)
            return
        self.assertTrue(False)
    
    def testSendSnapshotSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        snapshot = dataset+'@zfsbackup-sendtest'
        destination = self.base_dataset+'/'+self.dest_dataset
        zfsbackup.create_snapshot(dataset,'zfsbackup-sendtest')
        zfsbackup.send_snapshot(snapshot, destination,transport="ssh:root@localhost")
        snaps = zfsbackup.get_snapshots(destination)
        self.assertTrue(destination+'@zfsbackup-sendtest' in snaps)

    def testSendSnapshotSSHAlt(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        snapshot = dataset+'@zfsbackup-sendtest'
        destination = self.base_dataset+'/'+self.dest_dataset
        zfsbackup.create_snapshot(dataset,'zfsbackup-sendtest')
        zfsbackup.send_snapshot(snapshot, destination,transport="ssh:root@localhost:44")
        snaps = zfsbackup.get_snapshots(destination)
        self.assertTrue(destination+'@zfsbackup-sendtest' in snaps)

    def testSendFullLocal(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/'+self.dest_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_full(dataset+snap,dest)
        snaps = zfsbackup.get_snapshots(dest)
        self.assertTrue(dest+snap in snaps)

    def testSendFullSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/'+self.dest_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_full(dataset+snap,dest,transport="ssh:root@localhost")
        snaps = zfsbackup.get_snapshots(dest)
        self.assertTrue(dest+snap in snaps)

    def testSendFullSSHAlt(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/'+self.dest_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_full(dataset+snap,dest,transport="ssh:root@localhost:44")
        snaps = zfsbackup.get_snapshots(dest)
        self.assertTrue(dest+snap in snaps)

    def testVerifyBackupLocal(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/'+self.dest_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_full(dataset+snap,dest)
        self.assertTrue(zfsbackup.verify_backup(snap,dest,'local'))

    def testVerifyBackupSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/'+self.dest_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_full(dataset+snap,dest)
        self.assertTrue(zfsbackup.verify_backup(snap,dest,'ssh:root@localhost'))

    def testVerifyBackupFail(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        snap = '@obviously-fake'
        try:
            result = zfsbackup.verify_backup(snap,dataset,'local')
            self.assertFalse(result)
        except ZFSBackupError:
            pass

    def testSendIncrementalLocal(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/'+self.dest_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_full(dataset+snap,dest)
        time.sleep(1)
        snap_inc = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_incremental(dataset+snap,dataset+snap_inc,dest)
        self.assertTrue(zfsbackup.verify_backup(snap_inc,dest,'local'))
       
    def testSendIncrementalSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/'+self.dest_dataset
        snap = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_full(dataset+snap,dest,transport='ssh:root@localhost')
        time.sleep(1)
        snap_inc = zfsbackup.create_timestamp_snap(dataset)
        zfsbackup.send_incremental(dataset+snap,dataset+snap_inc,dest,transport='ssh:root@localhost')
        self.assertTrue(zfsbackup.verify_backup(snap_inc,dest,'ssh:root@localhost'))

    def testBackupDatasetLocal(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'local'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception "+e.message)

    def testBackupDatasetSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'ssh:root@localhost'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception: "+e.message)

    def testBackupDatasetIncrementalLocal(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'local'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
            time.sleep(1)
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception "+e.message)

    def testBackupDatasetIncrementalSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'ssh:root@localhost'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
            time.sleep(1)
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception "+e.message)
    
    def testBackupDatasetMultipleLocal(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'local'},{'dest':self.base_dataset+'/destination2','transport':'local'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception: "+e.message)
    
    def testBackupDatasetMultipleSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'ssh:root@localhost'},{'dest':self.base_dataset+'/destination2','transport':'ssh:root@localhost'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception: "+e.message)

    def testBackupDatasetMultipleIncrementalLocal(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'local'},{'dest':self.base_dataset+'/destination2','transport':'local'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
            time.sleep(1)
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception: "+e.message)

    def testBackupDatasetMultipleIncrementalSSH(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = [{'dest':self.base_dataset+'/'+self.dest_dataset,'transport':'ssh:root@localhost'},{'dest':self.base_dataset+'/destination2','transport':'ssh:root@localhost'}]
        try:
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
            time.sleep(1)
            zfsbackup.backup_dataset(dataset,dest,'@zfsbackup-last')
        except ZFSBackupError as e:
            self.fail("caught exception: "+e.message)

    def testSendSnapshotBadDest(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        dest = self.base_dataset+'/non-existent-dataset/butreally'
        zfsbackup.create_snapshot(dataset, 'zfsbackup-sendtest')
        try:
            zfsbackup.send_snapshot(dataset+'@zfsbackup-sendtest',dest)
        except ZFSBackupError:
            return
        self.fail("Should have caught an exception")

    def testSendSnapshotBadSrc(self):
        dataset = self.base_dataset+'/deffo-real'
        dest = self.base_dataset+'/'+self.dest_dataset
        try:
            zfsbackup.send_snapshot(dataset+'@ohyeahthissnapisreal',dest)
        except ZFSBackupError:
            return
        except ResourceWarning:
            self.fail("Caught a ResourceWarning")
        self.fail("Should have caught an exception")

    def testSendSnapshotBadDestLarge(self):
        dataset = self.base_dataset+'/'+self.source_dataset
        generateLargeFile('/'+dataset+'/largefile',1)
        dest = self.base_dataset+'/non-existent-dataset/butreally'
        zfsbackup.create_snapshot(dataset, 'zfsbackup-sendtest')
        try:
            zfsbackup.send_snapshot(dataset+'@zfsbackup-sendtest',dest)
        except ZFSBackupError:
            return
        self.fail("Should have caught an exception")

    def testDestSnapshotDeleteLocal(self):
        dataset = self.base_dataset+'/'+self.dest_dataset
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        saved_penultimate = zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        saved_last = zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.clean_dest_snaps([{'dest': dataset, 'transport': 'local'}], 2)
        zfsbackup.verify_backup(saved_penultimate, dataset, 'local')
        zfsbackup.verify_backup(saved_last, dataset, 'local')


    def testDestSnapshotDeleteSSH(self):
        dataset = self.base_dataset+'/'+self.dest_dataset
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        saved_penultimate = zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        saved_last = zfsbackup.create_timestamp_snap(dataset)
        time.sleep(1)
        zfsbackup.clean_dest_snaps([{'dest': dataset, 'transport': 'ssh:root@localhost'}], 2)
        zfsbackup.verify_backup(saved_penultimate, dataset, 'ssh:root@localhost')
        zfsbackup.verify_backup(saved_last, dataset, 'ssh:root@localhost')


    def testValidateConfig(self):
        # do more than this
        c = zfsbackup.validate_config('../config_example.yml')
        self.assertTrue('log_file' in c)
        self.assertTrue('lock_file' in c)
        for ds in c.get('datasets'):
            self.assertTrue('dataset_name' in ds)
            for t in ds.get('destinations'):
                self.assertTrue(('dest' in t) and ('transport' in t))


if __name__ == '__main__':
    unittest.main()
