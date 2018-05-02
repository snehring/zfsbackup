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
    #TODO: argparse setup
    args = argparse.ArgumentParser(description="Program to automatically create and send snapshots of zfs datasets")
    args.add_argument('--config', inargs='?' help='path to file containing configuration for %(prog)s', type=String)
    args.add_argument('dataset', type=String, help='name of dataset to replicate')
    args.add_argument('destination', type=String, help='where to send the dataset')
    #default lockfile location"
    lf_path = "/var/lock/zfsbackup.lock"
    #TODO: config file parsing. Decide on format.
    lf_fd = create_lockfile(lf_path)
    #TODO: for each dataset
    #           check for left over timestamp snaps exit if found, loudly
    #           check has_backuplast()
    #           yes? create timestamp snap for now, do incremental, check destination for timestamp snap, remove old backup-last, rename timestamp snap to backup-last
    #           no? create timestamp snap for now, do send, check destination for timestamp snap, rename timestamp snap to backup-last
    #TODO: determine if we want a 'retry queue' of failed datasets, if so, make sure those are added into the failure queue above

"""Create a snapshot of the given dataset with the specified name"""
def create_snapshot(dataset, name):
    try:
        zfs = subprocess.run(['zfs', 'snap', dataset+'@'+name], timeout=10, stderr=subprocess.PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        #returned non-zero
        logging.error("Unable to create snapshot "+dataset+'@'+name)
        logging.error("Got: "+__cleanup_stdout(e.stderr))
        raise ZFSBackupError("Failed to create snapshot "+dataset+'@'+name)
    except TimeoutExpired:
        #timed out
        logging.error("Failed to create snapshot "+dataset+'@'+name". Timeout reached.")
        raise ZFSBackupError("Failed to create snapshot "+dataset+'@'+name)

"""Create a snapshot with the backup-YYYYMMDD-HHMM name format"""
def create_timestamp_snap(dataset):
    # call create_snapshot with correct name
    timestamp = datetime.now().strftime('%Y%m%d-%H%M')
    create_snapshot(dataset,'backup-'+timestamp)

"""delete snapshot specified by snapshot.
   specified name should literally be the name returned by
   zfs list -t snap"""
def delete_snapshot(snapshot):
    try:
        zfs = subprocess.run(['zfs', 'destroy', snapshot], timeout=120, stderr=subprocess.PIPE, check=True, encoding='utf-8')
    except CalledProcessError as e:
        #returned non-zero
        logging.error("Unable to destroy snapshot "+snapshot)
        logging.error("Got: "+__cleanup_stdout(e.stderr))
        raise ZFSBackupError("Failed to delete snapshot " +snapshot)
    except TimeoutExpired:
        #timed out
        logging.error("Unable to destroy snapshot "+snapshot+". Timeout reached.")

"""Renames a snapshot to newname"""
def rename_snapshot(snapshot, newname):
    #TODO: rename snapshot

"""Do a full send of snapshot specified by snapshot to destination
   using transport. If transport is None, it's assumed to be local"""
def send_snapshot(snapshot, destination, transport=None):
    #TODO: send snapshot

"""Same as send_snapshot(), but do an incremental between
   snapshot1 and snapshot2"""
def send_incremental(snapshot1, snapshot2, destination, transport=None):
    #TODO

"""Returns true if dataset has stragler backup-<datestamp> snapshots"""
def has_straglers(dataset):
    snaps = get_snapshots(dataset)
    regex = re.compile(".*@backup-\d\d\d\d\d\d\d\d-\d\d\d\d")
    # this is likely not the best way to do this, but it shouldn't be too awful
    matches = list(filter(regex.match, snaps))
    if matches:
        return True
    else:
        return False

"""returns a python list of snapshots for a dataset"""
def get_snapshots(dataset):
    #get list of snapshots
    try:
        zfs = subprocess.run(['zfs','list', '-H', '-r', '-t', 'snapshot', '-o', 'name', dataset], stdout=subprocess.PIPE,stderr=subprocess.PIPE, check=True,timeout=10, encoding='utf-8')
        # remove empty lines and return a list with the contents of stdout
        snaps = __cleanup_stdout(zfs.stdout)
        return snaps
    except CalledProcessError as e:
        # command returned non-zero error code
        logging.error("Unable to get list of snapshots for "+dataset+". 
                      zfs list returned non-zero return code.")
        logging.error("Got: "+__cleanup_stdout(e.stderr))
       raise ZFSBackupError("Unable to get list of snapshots for "+dataset)
    except TimeoutExpired:
        # command timed out
        logging.error("Unable to get list of snapshots for "+dataset+". Timeout reached.")
        raise ZFSBackupError("Unable to get list of snapshots for "+dataset)

"""return true if dataset has a backup-last snapshot"""
def has_backuplast(dataset):
    snaps = get_snapshots(dataset)
    if dataset+'@backup-last' in snaps:
        return True
    else:
        return False

"""Atomically create a lockfile, exit if not possible.
   returns file object coresponding to path."""
def create_lockfile(path):
    try:
        return os.open(path,os.O_CREAT|os.O_EXCL)
    except FileExistsError:
        # file already exists, another instance must be running
        logging.critical("Error: lock file "+path+" already exists. Exiting.")
        sys.exit("Lock file exists.")
    except OSError as e:
        #We're unable to create the file for whatever reason. Report it. Exit.
        logging.critical("Error: Unable to create lock file.")
        logging.critical(e)
        sys.exit("Unable to open lock file")
    except as e:
        # some other error has occured, report it and exit.
        logging.critical("Error: unable to get open lock file. Error was"+e+)
        sys.exit("Unable to open lock file.")
"""Clean up lockfile"""
def clean_lockfile(lockfile):
    #close and remove the lockfile.
    try:
        os.close(lockfile)
        os.remove(lockfile)
    except OSError as e:
        logging.warning("Unable to clean up lockfile.")
        logging.warning(e)

"""Removes empty elements from the stdout/stderr list returned by run"""
def __cleanup_stdout(stdout):
    return list(filter(None, stdout.split('\n')))

class ZFSBackupError(Exception):
    """Exception for this program."""
    def __init__(self, message):
        self.message = message
