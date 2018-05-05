import unittest
import zfsbackup
import os


class TestZFSBackup(unittest.TestCase):
    """
       Tests for the zfsbackup.py utility
    """
    # this is the base dataset we will be testing in, assumed to already exist
    # assumed that no one will care if we destroy everything in it
    # please adjust as needed
    base_dataset = "store/zfs_backup"
    source_dataset = "source"
    dest_dataset = "destination"

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

if __name__ == '__main__':
    unittest.main()
