import argparse
import logging
import subprocess
import re
import os
import sys
from datetime import datetime
import yaml


def main():
    # TODO: argparse setup
    ap_desc = """Program to automatically create and send snapshots of zfs
                 datasets."""
    arg_parser = argparse.ArgumentParser(description=ap_desc)
    arg_parser.add_argument('-c', '--config',
                            help='path to configuration for %(prog)s',
                            type=str)
    arg_parser.add_argument('dataset', type=str,
                            help='name of dataset to replicate')
    arg_parser.add_argument('destination', type=str,
                            help='where to send the dataset')
    arg_parser.add_argument('transport', type=str,
                            help='how to send the dataset, local or ssh. '
                            + 'If not provided, local assumed.')
    args = arg_parse.parse_args()
    if args.dataset or args.destination:
        # single dataset run
        if not dataset and destination:
            logging.error("Please provide both a dataset and a destination")
            sys.exit("Invalid argument")
        # TODO: single dataset run logic
    else:
        #config run
        if not os.path.exists(args.config):
            logging.error("Cannot find config file at "+args.config)
            sys.exit("Config file not found".)
        conf = validate_config(args.config)
        if conf.get('log_file'):
            # set log file 
            # if the path is invalid or not writable I bet this'll complain
            # and that's fine, I'd percolate up any exception anyway
            logging.basicConfig(filename=conf.get('log_file')) 
        # TODO future: user selectable logging levels
        logging.getLogger().setLevel(logging.INFO)
        # do whatever based on assumed good file


    # default lockfile location"
    lf_path = "/var/lock/zfsbackup.lock"
    lf_fd = create_lockfile(lf_path)
    # TODO: for each dataset
    #           check for left over timestamp snaps exit if found, loudly
    #           check has_backuplast()
    #           # TODO: if doing n destinations, check for mulitple dests
    #           # if so process each destination before snapshot rename
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
    # TODO: clean up and exit
    clean_lockfile(lf_fd)
    sys.exit()


def validate_config(conf_path):
    """Peforms basic validation of config file format.
       I hope for your sake the actual dataset and destination paths
       are correct."""
       # TODO: verify perms of file
    with open(args.config) as conf_f:
        try:
            conf = yaml.load(conf_f.read())
        except yaml.YAMLError as e:
            # parsing error
            logging.error("Invalid config file.")
            raise e
    if not conf.get('datasets'):
        logging.error("Error: no datasets defined, or defined incorrectly")
        raise ZFSBackupError("Invalid config file.")
        for d in conf.get('datasets'):
            if (not d) or (not d.get('dataset_name')) or (not d.get('destinations'))
            logging.error("Error: dataset config incorrectly defined.")
            raise ZFSBackupError("Invalid config file.")
            for l in d.get('destinations'):
                if (not l) or (not l.get('dest')):
                    logging.error("Error: destination config incorrectly " 
                                  + "defined for: "+d.get('dataset_name'))
                    ZFSBackupError("Invalid config file.")
    return conf


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
        raise ZFSBackupError("Failed to rename dataset: "+dataset+" newname: "
                             + newname)
    except TimeoutExpired:
        # timed out
        logging.error("Unable to rename dataset "+dataset+". Timeout Reached.")
        raise ZFSBackupError("Failed to rename dataset "+dataset)


def rename_snapshot(snapshot, newname):
    """Renames a snapshot to newname"""
    # check that it's a snapshot
    if ('@' not in snapshot) or ('@' not in newname):
        logging.error("Error: tried to rename a non-snapshot or rename a"
                      + "snapshot to a non-snapshot."
                      + "Snapshot was: "+snapshot+"and newname was: "
                      + newname)
        raise ZFSBackupError(
            "Tried to rename a snapshot incorrectly. snapshot: "
            + snapshot+" newname: "+newname)
    # call the function to actually rename
    rename_dataset(snapshot, newname)


def send_snapshot(snapshot, destination, transport='local',
                  incremental_source=None):
    """Send a snapshot to a destination using transport.
    snapshot is the full zfs path of the snapshot
    destination is the full zfs path of the destination to be recv'd into
    If incremental send, provide a source.
    If transport is not provided, it's assumed to be local.
    currently only local and ssh are supported as transports. ssh
    transport has form 'ssh:user@hostname<:port>'"""
    if '@' not in snapshot:
        logging.error("Error: tried to send non snapshot "+snapshot)
        raise ZFSBackupError("Tried to send non-snapshot "+snapshot)
    if incremental_source:
        if '@' not in incremental_source:
            logging.error("Error: incremental_source is not a snapshot. snap: "
                          + snapshot+"dest: "+destination+" inc_source: "
                          + incremental_source)
            raise ZFSBackupError("incremental_source not a snapshot. snap: "
                                 + snapshot+" dest: "+destination
                                 + " inc_source: "+incremental_source)
        zsend_command = ['zfs', 'send', '-ec', '-i', incremental_source,
                         snapshot]
    else:
        zsend_command = ['zfs', 'send', '-ec', snapshot]
    zrecv_command = ['zfs', 'recv', '-F', destination]
    if transport.lower() == 'local':
        zfs_send = subprocess.Popen(zsend_command, stdout=subprocess.PIPE)
        zfs_recv = subprocess.Popen(zrecv_command, stdin=zfs_send.stdout)
        try:
            zfs_recv.communicate()
            zfs_recv.wait()
            zfs_send.wait()
        except Exception as e:
            zfs_send.kill()
            zfs_recv.kill()
            logging.error("Error: exception while sending: "+e)
            raise ZFSBackupError("Caught an exception while sending "+e)
        if (zfs_send.returncode != 0) or (zfs_recv != 0):
            # we failed somewhere
            logging.error("Error: send of "+snapshot+" to "
                          + destination+" failed.")
            raise ZFSBackupError("Send of "+snapshot+" to "
                                 + destination+" failed.")
        logging.info("Finished send of "+snapshot+" via <"
                     + transport.lower()+"> to "+destination)
    elif transport.lower().split(':')[0] == 'ssh':
        port = '22'
        if len(transport.split(':')) > 2:
            # assume that the 3rd element is a port number
            port = transport.split(':')[2]
        zfs_send = subprocess.Popen(zsend_command, stdout=subprocess.PIPE)
        # TODO: have a configurable for ssh-key instead of just assuming
        username, hostname = transport.split(':')[1].split('@')
        ssh_remote_command = "zfs recv -F "+destination
        ssh_command = ['ssh', '-o', 'PreferredAuthentications=publickey',
                       '-o', 'PubkeyAuthentication=yes', '-p', port, '-l',
                       username, hostname, ssh_remote_command]
        ssh_recv = subprocess.Popen(ssh_command, stdin=zfs_send.stdout)
        try:
            ssh_recv.communicate()
            ssh_recv.wait()
            zfs_send.wait()
        except Exception as e:
            zfs_send.kill()
            ssh_recv.kill()
            logging.error("Error: exception caught while sending: "+e)
            raise ZFSBackupError("Caught an exception while sending "+e)
        if (zfs_send.returncode != 0) or (ssh_recv != 0):
            # we failed somewhere
            logging.error("Error: send of "+snapshot+" to "
                          + destination+" failed.")
            raise ZFSBackupError("Send of "+snapshot+" to "
                                 + destination+" failed.")
        logging.info("Finished send of "+snapshot+"via <"
                     + transport.lower()+"> to "+destination)
    else:
        # some transport we don't support
        # shouldn't happen with config parsing
        # handle it anyway
        logging.error("Error: invalid transport specified: "+transport)
        raise ZFSBackupError("Invalid transport: "+transport)


def send_full(snapshot, destination, transport='local'):
    """Do a full send of snapshot specified by snapshot to destination
    using transport. If transport is not provided, it's assumed to be local.
    currently only local and ssh are supported as transports. ssh
    transport has form 'ssh:user@hostname<:port>'"""
    send_snapshot(snapshot, destination, transport=transport)


def send_incremental(snapshot1, snapshot2, destination, transport='local'):
    """Same as send_snapshot(), but do an incremental between
   snapshot1 and snapshot2, with snapshot1 being the incremental_source
   (earlier) snapshot and snapshot2 being the incremental_target (later)
   snapshot"""
    send_snapshot(snapshot2, destination, transport=transport,
                  incremental_source=snapshot1)


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
        logging.error("Unable to get list of snapshots for " + dataset
                      + ". zfs list returned non-zero return code.")
        logging.error("Got: "+__cleanup_stdout(e.stderr))
        raise ZFSBackupError("Unable to get list of snapshots for "+dataset)
    except TimeoutExpired:
        # command timed out
        logging.error("Unable to get list of snapshots for "
                      + dataset+". Timeout reached.")
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
    except Exception as e:
        # some other error has occured, report it and exit.
        logging.critical("Error: unable to get open lock file. Error was"+e)
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


if sys.version_info[0] != 3 or sys.version_info[1] < 6:
    print("This program requires at least Python 3.6")
    sys.exit("Wrong Python")
if __name__ == "__main__":
    main()
