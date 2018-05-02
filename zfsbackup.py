import argparse
import logging
import subprocess
import re
import os
import sys
from datetime import datetime

if sys.version_info[0] != 3 or sys.version_info[1] < 6:
    print("This program requires at least Python 3.6")
    sys.exit("Wrong Python")
if __name__ == "__main__":
    main()


def main():
    # TODO: argparse setup
    args = argparse.ArgumentParser(
        description='Program to automatically create and send snapshots of zfs datasets')
    args.add_argument('--config', inargs='?',
                      help='path to configuration for %(prog)s',
                      type=String)
    args.add_argument('dataset', type=String,
                      help='name of dataset to replicate')
    args.add_argument('destination', type=String,
                      help='where to send the dataset')
    # default lockfile location"
    lf_path = "/var/lock/zfsbackup.lock"
    # TODO: config file parsing. Decide on format.
    lf_fd = create_lockfile(lf_path)
    # TODO: for each dataset
    #           check for left over timestamp snaps exit if found, loudly
    #           check has_backuplast()
    #           yes?
    #               create timestamp snap for now
    #               do incremental
    #               check destination for timestamp snap
    #               remove old backup-last
    #               rename timestamp snap to backup-last
    #           no?
    #               create timestamp snap for now
    #               do send
    #               check destination for timestamp snap
    #               rename timestamp snap to backup-last
    # TODO: determine if we want a 'retry queue' of failed datasets
    # if so, make sure those are added into the failure queue above


def create_snapshot(dataset, name):
    """Create a snapshot of the given dataset with the specified name"""
    try:
        zfs = subprocess.run(['zfs', 'snap', dataset+'@'+name], timeout=10,
                             stderr=subprocess.PIPE, check=True,
                             encoding='utf-8')
    except CalledProcessError as e:
        # returned non-zero
        logging.error("Unable to create snapshot "+dataset+'@'+name)
        logging.error("Got: "+__cleanup_stdout(e.stderr))
        raise ZFSBackupError("Failed to create snapshot "+dataset+'@'+name)
    except TimeoutExpired:
        # timed out
        logging.error("Failed to create snapshot " + dataset + '@' + name +
                      ". Timeout reached.")
        raise ZFSBackupError("Failed to create snapshot " + dataset +
                             '@' + name)


def create_timestamp_snap(dataset):
    """Create a snapshot with the backup-YYYYMMDD-HHMM name format"""
    # call create_snapshot with correct name
    timestamp = datetime.now().strftime('%Y%m%d-%H%M')
    create_snapshot(dataset, 'backup-'+timestamp)


def delete_snapshot(snapshot):
    """delete snapshot specified by snapshot.
   specified name should literally be the name returned by
   zfs list -t snap"""
    # try to make sure we're not deleting anything other than a snapshot
    if '@' not in snapshot:
        logging.error("Error: tried to delete "+snapshot +
                      " which seems to not be a snapshot")
        raise ZFSBackupError(
            "Tried to delete something other than a snapshot. Was: "+snapshot)
    try:
        zfs = subprocess.run(['zfs', 'destroy', snapshot], timeout=120,
                             stderr=subprocess.PIPE, check=True,
                             encoding='utf-8')
    except CalledProcessError as e:
        # returned non-zero
        logging.error("Unable to destroy snapshot "+snapshot)
        logging.error("Got: "+__cleanup_stdout(e.stderr))
        raise ZFSBackupError("Failed to delete snapshot "+snapshot)
    except TimeoutExpired:
        # timed out
        logging.error("Unable to destroy snapshot " +
                      snapshot+". Timeout reached.")
        raise ZFSBackupError("Failed to delete snapshot "+snapshot)


def rename_dataset(dataset, newname):
    """Renames a dataset to newname"""
    try:
        zfs = subprocess.run(['zfs', 'rename', dataset, newname],
                             sterr=subprocess.PIPE, check=True, timeout=10,
                             encoding='utf-8')
    except CalledProcessError as e:
        # command returned non-zero error code
        logging.error("Error: Unable to rename dataset "+dataset+"to "+newname)
        logging.error("Got: "+__cleanup_stdout(e.stderr))
        raise ZFSBackupError("Failed to rename dataset: "+dataset" newname: "
                             + newname)
    except TimeoutExpired:
        # timed out
        logging.error("Unable to rename dataset "+dataset+". Timeout Reached.")
        raise ZFSBackupError("Failed to rename dataset "+dataset)


def rename_snapshot(snapshot, newname):
    """Renames a snapshot to newname"""
    # check that it's a snapshot
    if '@' (not in snapshot) or (not in newname):
        logging.error("Error: tried to rename a non-snapshot or rename a"
                      + "snapshot to a non-snapshot."
                      + "Snapshot was: "+snapshot+"and newname was: "
                      + newname)
        raise ZFSBackupError(
            "Tried to rename a snapshot incorrectly. snapshot: "
            + snapshot+" newname: "+newname)
    # call the function to actually rename
    rename_dataset(snapshot, newname)


def send_snapshot(snapshot, destination, transport=None):
    """Do a full send of snapshot specified by snapshot to destination
   using transport. If transport is None, it's assumed to be local"""
    # TODO: send snapshot


def send_incremental(snapshot1, snapshot2, destination, transport=None):
    """Same as send_snapshot(), but do an incremental between
   snapshot1 and snapshot2"""
    # TODO


def has_straglers(dataset):
    """Returns true if dataset has stragler backup-<datestamp> snapshots"""
    snaps = get_snapshots(dataset)
    regex = re.compile(".*@backup-\d{8}-\d{4}")
    # this is likely not the best way to do this, but it shouldn't be too awful
    matches = list(filter(regex.match, snaps))
    if matches:
        return True
    else:
        return False


def get_snapshots(dataset):
    """returns a python list of snapshots for a dataset"""
    # get list of snapshots
    try:
        zfs = subprocess.run(['zfs', 'list', '-H', '-r', '-t', 'snapshot',
                              '-o', 'name', dataset], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, check=True, timeout=10,
                             encoding='utf-8')
        # remove empty lines and return a list with the contents of stdout
        snaps = __cleanup_stdout(zfs.stdout)
        return snaps
    except CalledProcessError as e:
        # command returned non-zero error code
        logging.error("Unable to get list of snapshots for " + dataset + ".
                      zfs list returned non-zero return code.")
        logging.error("Got: "+__cleanup_stdout(e.stderr))
        raise ZFSBackupError("Unable to get list of snapshots for "+dataset)
    except TimeoutExpired:
        # command timed out
        logging.error("Unable to get list of snapshots for " +
                      dataset+". Timeout reached.")
        raise ZFSBackupError("Unable to get list of snapshots for "+dataset)


def has_backuplast(dataset):
    """return true if dataset has a backup-last snapshot"""
    snaps = get_snapshots(dataset)
    if dataset+'@backup-last' in snaps:
        return True
    else:
        return False


def create_lockfile(path):
    """Atomically create a lockfile, exit if not possible.
   returns file object coresponding to path."""
    try:
        # on linux (the only place this will be used...I hope)
        # according to man 2 open, open with O_CREAT and O_EXCL
        # will fail if the file already exists
        # this gives us an easy atomic lockfile check/create
        return os.open(path, os.O_CREAT | os.O_EXCL)
    except FileExistsError:
        # file already exists, another instance must be running
        logging.critical("Error: lock file "+path+" already exists. Exiting.")
        sys.exit("Lock file exists.")
    except OSError as e:
        # We're unable to create the file for whatever reason. Report it. Exit.
        logging.critical("Error: Unable to create lock file.")
        logging.critical(e)
        sys.exit("Unable to open lock file")
    except as e:
        # some other error has occured, report it and exit.
        logging.critical("Error: unable to get open lock file. Error was"+e+)
        sys.exit("Unable to open lock file.")


def clean_lockfile(lockfile):
    """Clean up lockfile"""
    # close and remove the lockfile.
    try:
        os.close(lockfile)
        os.remove(lockfile)
    except OSError as e:
        logging.warning("Unable to clean up lockfile.")
        logging.warning(e)


def __cleanup_stdout(stdout):
    """Removes empty elements from the stdout/stderr list returned by run"""
    return list(filter(None, stdout.split('\n')))


class ZFSBackupError(Exception):
    """Exception for this program."""

    def __init__(self, message):
        self.message = message
