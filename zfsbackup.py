"""
   zfsbackup.py a simple zfs backup utility
"""
import argparse
import logging
import subprocess
from subprocess import CalledProcessError, TimeoutExpired
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
    arg_parser.add_argument('dataset', type=str, nargs='?',
                            help='name of dataset to replicate')
    arg_parser.add_argument('destination', type=str, nargs='?',
                            help='where to send the dataset')
    arg_parser.add_argument('transport', type=str, nargs='?', default='local',
                            help='how to send the dataset, local or ssh. '
                            + 'If not provided, local assumed.'
                            + 'ssh format: '
                            + 'ssh:username@hostname<:port>')
    args = arg_parser.parse_args()
    # hard coded if you don't provide one in the config file, sorry.
    lf_path = "/var/lock/zfsbackup.lock"
    # TODO: make this user customizable
    incremental_name = "@zfsbackup-last"
    # error counter
    errors = 0
    if args.dataset or args.destination:
        # single dataset run
        if not args.dataset and args.destination:
            logging.error("Please provide both a dataset and a destination")
            return -1
        # lockfile
        try:
            lf_fd = create_lockfile(lf_path)
        except Exception:
            logger.critical("Exiting: cannot get a lockfile")
            return -2
        name = args.dataset
        dest = args.destination
        transport = args.transport
        if has_straglers(name):
            logging.warn("Dataset: "+name+"has left over temporary "
                         + "snapshots. IT WAS NOT BACKED UP! You need "
                         + "to resolve this manually. Make sure everything "
                         + "is consistent and remove the left over "
                         + "zfsbackup-yyymmdd-hhmm snaps.")
            return -1
        else:
            try:
                backup_dataset(name, dest, incremental_name, transport)
            except ZFSBackupException as e:
                logging.warn("Dataset backup of "+name+" to "+dest
                             + "FAILED! YOU'LL WANT TO SEE TO THAT!")
                errors += 1
    elif args.config:
        # config run
        if not os.path.exists(args.config):
            logging.error("Exiting: Cannot find config file at "+args.config)
            return -1
        conf = validate_config(args.config)
        if conf.get('log_file'):
            # set log file
            # if the path is invalid or not writable I bet this'll complain
            # and that's fine, I'd percolate up any exception anyway
            logging.basicConfig(filename=conf.get('log_file'))
        if conf.get('lock_file'):
            lf_path = conf.get('lock_file')
        # TODO future: user selectable logging levels
        logging.getLogger().setLevel(logging.INFO)
        # create lockfile
        try:
            lf_fd = create_lockfile(lf_path)
        except Exception:
            logger.critical("Exiting: cannot get a lockfile.")
            return -1
        for ds in conf.get('datasets'):
            name = ds.get('name')
            if has_straglers(name):
                logging.warn("Dataset: "+name+"has left over temporary "
                             + "snapshots. IT WAS NOT BACKED UP! You need "
                             + "to resolve this manually. Make sure "
                             + "everything is consistent and remove "
                             + "the left over zfsbackup-yyyymmdd-hhmm snaps.")
                continue
            else:
                for dst in ds.get('destinations'):
                    try:
                        backup_dataset(name, dst.get('dest'), incremental_name,
                                       dst.get('transport'))
                    except ZFSBackupException as e:
                        logging.warn("Dataset backup of "+name+" to "
                                     + dst.get('dest')+" via "
                                     + dst.get('transport')+" FAILED!"
                                     + "YOU'LL WANT TO SEE TO THAT!")
                        errors += 1
    elif not args.config:
        # config file not provided
        logging.error("Config file required if no other arguments given.")
        return -1
    else:
        # we shouldn't ever get here
        logging.error("Woops, I guess I broke argument parsing")
        return -128

    # TODO: determine if we want a 'retry queue' of failed datasets
    # if so, make sure those are added into the failure queue above
    clean_lockfile(lf_path, lf_fd)
    if errors > 0:
        return -10
    else:
        return 0


def validate_config(conf_path):
    """Peforms basic validation of config file format.
       I hope for your sake the actual dataset and destination paths
       are correct.
       :param conf_path: path to the config file
       :return: returns the validated yaml file as a python object"""
    # TODO: verify perms of file
    with open(conf_path) as conf_f:
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
            if not d or not d.get('dataset_name') or not d.get('destinations'):
                logging.error("Error: dataset config incorrectly defined.")
                raise ZFSBackupError("Invalid config file.")
            for l in d.get('destinations'):
                if (not l) or (not l.get('dest')):
                    logging.error("Error: destination config incorrectly "
                                  + "defined for: "+d.get('dataset_name'))
                    ZFSBackupError("Invalid config file.")
    return conf


def backup_dataset(dataset, destination, inc_snap, transport='local'):
    """Backup a dataset to the specified destination using the specified
       transport.
       param dataset: dataset to be backed up
       param destination: destination dataset
       param inc_snap: the incremental source snapshot
       param transport: the method to send the zfs stream
       raises: ZFSBackupError"""
    try:
        new_snap = create_timestamp_snap(dataset)
        if has_backuplast(dataset, inc_snap):
            # do incremental
            send_incremental(dataset+inc_snap, dataset+new_snap,
                             destination, transport=transport)
            logging.info("Incremental send of "+dataset+new_snap+" to "
                         + destination+" via "+transport+" finished.")
            # delete old incremental marker
            try:
                delete_snapshot(dataset+inc_snap)
                logging.info("Deleted old incremental snapshot")
            except ZFSBackupError as e:
                logging.error("Unable to delete "+dataset+inc_snap
                              + " YOU NEED TO DELETE THAT AND THEN RENAME "
                              + dataset+new_snap+" TO "+dataset+inc_snap)
            raise e
        else:
            # do full send
            send_full(dataset+new_snap, destination, transport=transport)
            logging.info("Full send of "+dataset+new_snap+" to " + destination
                         + " via "+transport+" finished.")
        if verify_backup(new_snap, destination, transport):
            # good backup
            logging.info("Verification of "+destination+new_snap+" via "
                         + transport+" finished.")
        else:
            # bad backup exit
            logging.error("Verification of "+destination+new_Snap+" via"
                          + transport+" FAILED!")
            try:
                delete_snapshot(dataset+new_snap)
            except Exception as e:
                logging.error("Unable to clean up snapshot "+dataset+new_snap
                              + " after failed verification.")
            finally:
                raise ZFSBackupError("Verification of "+destination+new_snap
                                     + " FAILED!")
        # rename dataset+new_snap to dataset+inc_snap
        try:
            rename_snapshot(dataset+new_snap, dataset+inc_snap)
            logging.info("Rename of " + dataset+new_snap+" to "
                         + dataset+inc_snap+" finished.")
        # done
        except ZFSBackupError as e:
            logging.error("UNABLE TO RENAME"+dataset+new_snap+" TO "
                          + dataset+inc_snap+" YOU NEED TO DO THIS MANUALLY!")
            raise e
    except ZFSBackupError as e:
        logging.error("Failed backup of "+dataset+" to "+destination
                      + " via "+transport)
        raise e


def verify_backup(snapshot, destination, transport):
    """Verify backup is at destination
       param snapshot: snapshot that needs its presence verified
       param destination: where snapshot should be
       param transport: how to get to destination
       returns: True if the snapshot is present at destination, else False
       """
    try:
        if transport == 'local':
            zfs_command = ['zfs', 'list', '-H', '-t', 'snapshot',
                           '-o', 'name', destination+snapshot]
            zfs = subprocess.run(zfs_command, stderr=subprocess.PIPE,
                                 check=True, timeout=10, encoding='utf-8')
            return True
        elif transport.lower().split(':')[0] == 'ssh':
            port = '22'
            if len(transport.split(':')) > 2:
                # assume that the 3rd element is a port number
                port = transport.split(':')[2]
            # TODO: make the ssh communication it's own function probably
            username, hostname = transport.split(':')[1].split('@')
            zfs = "zfs list -H -t snapshot -o name "+destination+snapshot
            ssh_command = ['ssh', '-o', 'PreferredAuthentications=publickey',
                           '-o', 'PubkeyAuthentication=yes',
                           '-o', 'StrictHostKeyChecking=yes', '-p', port, '-l',
                           username, hostname, zfs]
            ssh = subprocess.run(ssh_command, stderr=subprocess.PIPE,
                                 check=True, timeout=10, encoding='utf-8')
            return True
        else:
            # crap we don't do
            return False
    except Exception as e:
        logging.Error("Unable to verify snap: "+snapshot+" exists at: "
                      + destination+" via "+transport)
        raise ZFSBackupError("Failed to verify backup of "+snapshot+" to "
                             + destination+" via "+transport)


def create_snapshot(dataset, name):
    """Create a snapshot of the given dataset with the specified name
       param dataset: dataset to snapshot
       param name: name of snapshot, sans '@'
       throws: ZFSBackupError if snapshot fails
       """
    try:
        zfs = subprocess.run(['zfs', 'snap', dataset+'@'+name], timeout=10,
                             stderr=subprocess.PIPE, check=True,
                             encoding='utf-8')
    except CalledProcessError as e:
        # returned non-zero
        logging.error("Unable to create snapshot "+dataset+'@'+name)
        logging.error("Got: "+str(__cleanup_stdout(e.stderr)))
        raise ZFSBackupError("Failed to create snapshot "+dataset+'@'+name)
    except TimeoutExpired:
        # timed out
        logging.error("Failed to create snapshot " + dataset + '@' + name +
                      ". Timeout reached.")
        raise ZFSBackupError("Failed to create snapshot " + dataset +
                             '@' + name)


def create_timestamp_snap(dataset):
    """Create a snapshot with the zfsbackup-YYYYMMDD-HHMM name format.
       returns name of created snapshot
       param dataset: dataset to create a timestamp snap of
       returns: string representing name of snapshot created
       throws: ZFSBackupError if snapshot fails
       """
    # call create_snapshot with correct name
    timestamp = datetime.now().strftime('%Y%m%d-%H%M')
    create_snapshot(dataset, 'zfsbackup-'+timestamp)
    return '@zfsbackup-'+timestamp


def delete_snapshot(snapshot):
    """delete snapshot specified by snapshot.
   specified name should literally be the name returned by
   zfs list -t snap
   param snapshot: snapshot to remove (dataset@name)
   throws ZFSBackupError if snapshot delete fails
   """
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
        logging.error("Got: "+str(__cleanup_stdout(e.stderr)))
        raise ZFSBackupError("Failed to delete snapshot "+snapshot)
    except TimeoutExpired:
        # timed out
        logging.error("Unable to destroy snapshot " +
                      snapshot+". Timeout reached.")
        raise ZFSBackupError("Failed to delete snapshot "+snapshot)


def rename_dataset(dataset, newname):
    """Renames a dataset to newname
       param dataset: dataset to be renamed
       param newname: new name of dataset
       throws: ZFSBackupError if rename fails
    """
    try:
        zfs = subprocess.run(['zfs', 'rename', dataset, newname],
                             stderr=subprocess.PIPE, check=True, timeout=10,
                             encoding='utf-8')
    except CalledProcessError as e:
        # command returned non-zero error code
        logging.error("Error: Unable to rename dataset "+dataset+"to "+newname)
        logging.error("Got: "+str(__cleanup_stdout(e.stderr)))
        raise ZFSBackupError("Failed to rename dataset: "+dataset+" newname: "
                             + newname)
    except TimeoutExpired:
        # timed out
        logging.error("Unable to rename dataset "+dataset+". Timeout Reached.")
        raise ZFSBackupError("Failed to rename dataset "+dataset)


def rename_snapshot(snapshot, newname):
    """Renames a snapshot to newname
       param snapshot: snapshot to be renamed
       param newname: new name of snapshot
       throws: ZFSBackupError if rename fails or if snapshot isn't a snapshot
    """
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
    transport has form 'ssh:user@hostname<:port>'
    param snapshot: snapshot to be sent
    param destination: where to send the snapshot
    param transport: how to send the snapshot
    param incremental_source: snapshot to use as the incremental source
    throws: ZFSBackup error if send fails, or snapshot params aren't snapshots
    """
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
        zfs_send = subprocess.Popen(zsend_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        zfs_recv = subprocess.Popen(zrecv_command, stdin=zfs_send.stdout, stderr=subprocess.PIPE)
        try:
            zfs_recv_stderr = zfs_recv.communicate()[1]
            zfs_send_stderr = zfs_send.stderr.read().decode('utf-8')
            zfs_recv_stderr = zfs_recv_stderr.decode('utf-8')
            zfs_recv.wait()
            zfs_send.wait()
            zfs_send.stderr.close()
            zfs_send.stdout.close()
        except Exception as e:
            zfs_recv.stderr.close()
            zfs_recv.kill()
            zfs_send.stdout.close()
            zfs_send.stderr.close()
            zfs_send.kill()
            logging.error("Error: exception while sending: "+str(e))
            raise ZFSBackupError("Caught an exception while sending "+str(e))
        if (zfs_send.returncode != 0) or (zfs_recv.returncode != 0):
            # we failed somewhere
            logging.error("Error: send of "+snapshot+" to "
                          + destination+" failed.")
            logging.error("zfs send stderr: "+zfs_send_stderr)
            logging.error("zfs recv stderr: "+zfs_recv_stderr)
            zfs_recv.kill()
            zfs_send.kill()
            raise ZFSBackupError("Send of "+snapshot+" to "
                                 + destination+" failed.")
        logging.info("Finished send of "+snapshot+" via <"
                     + transport.lower()+"> to "+destination)
    elif transport.lower().split(':')[0] == 'ssh':
        port = '22'
        if len(transport.split(':')) > 2:
            # assume that the 3rd element is a port number
            port = transport.split(':')[2]
        zfs_send = subprocess.Popen(zsend_command, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        # TODO: have a configurable for ssh-key instead of just assuming
        username, hostname = transport.split(':')[1].split('@')
        ssh_remote_command = "zfs recv -F "+destination
        ssh_command = ['ssh', '-o', 'PreferredAuthentications=publickey',
                       '-o', 'PubkeyAuthentication=yes', 
                       '-o', 'StrictHostKeyChecking=yes','-p', port, '-l',
                       username, hostname, ssh_remote_command]
        ssh_recv = subprocess.Popen(ssh_command, stdin=zfs_send.stdout,
                                    stderr=subprocess.PIPE)
        try:
            ssh_recv_stderr = ssh_recv.communicate()[1]
            zfs_send_stderr = zfs_send.stderr.read().decode('utf-8')
            ssh_recv_stderr = ssh_recv_stderr.decode('utf-8')
            ssh_recv.wait()
            zfs_send.wait()
            zfs_send.stderr.close()
            zfs_send.stdout.close()
        except Exception as e:
            ssh_recv.kill()
            zfs_send.stdout.close()
            zfs_send.stderr.close()
            zfs_send.kill()
            logging.error("Error: exception caught while sending: "+str(e))
            raise ZFSBackupError("Caught an exception while sending "+str(e))
        if (zfs_send.returncode != 0) or (ssh_recv.returncode != 0):
            # we failed somewhere
            ssh_recv.kill()
            zfs_send.kill()
            logging.error("Error: send of "+snapshot+" to "
                          + destination+" failed.")
            logging.error("zfs send stderr: "+zfs_send_stderr)
            logging.error("zfs recv stderr: "+zfs_recv_stderr)
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
    transport has form 'ssh:user@hostname<:port>'
    param snapshot: snapshot to send
    param destination: where to send snapshot
    param transport: how to send snapshot
    throws: ZFSBackupError if send fails
    """
    send_snapshot(snapshot, destination, transport=transport)


def send_incremental(snapshot1, snapshot2, destination, transport='local'):
    """Same as send_snapshot(), but do an incremental between
   snapshot1 and snapshot2, with snapshot1 being the incremental_source
   (earlier) snapshot and snapshot2 being the incremental_target (later)
   snapshot.
   param snapshot1: incremental source snap (earlier)
   param snapshot2: incremental target snap (later)
   param destination: where to send
   param transport: how to send
   """
    # TODO: should validate that snapshot1 is at destination, but eh
    send_snapshot(snapshot2, destination, transport=transport,
                  incremental_source=snapshot1)


def has_straglers(dataset):
    """Returns true if dataset has stragler zfsbackup-<datestamp> snapshots
       param dataset: dataset to check
       returns: True if straglers are found, False otherwise
       throws: ZFSBackupError if unable to get list of snapshots
    """
    snaps = get_snapshots(dataset)
    regex = re.compile(".*@zfsbackup-\d{8}-\d{4}")
    # this is likely not the best way to do this, but it shouldn't be too awful
    matches = list(filter(regex.match, snaps))
    if matches:
        return True
    else:
        return False


def get_snapshots(dataset):
    """returns a python list of snapshots for a dataset
       param dataset: dataset to enumerate snapshots for
       returns: list of snapshots
       throws: ZFSBackupError if unable to get list of snapshots
    """
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
        logging.error("Got: "+str(__cleanup_stdout(e.stderr)))
        raise ZFSBackupError("Unable to get list of snapshots for "+dataset)
    except TimeoutExpired:
        # command timed out
        logging.error("Unable to get list of snapshots for "
                      + dataset+". Timeout reached.")
        raise ZFSBackupError("Unable to get list of snapshots for "+dataset)


def has_backuplast(dataset, inc_name):
    """return true if dataset has a backup-last snapshot
       param dataset: dataset to check
       param inc_name: name of snapshot that is the last backup. Include '@'
       returns: True if the snapshot is found, False otherwise
       throws: ZFSBackupError if a list of snapshots cannot be obtained
    """
    snaps = get_snapshots(dataset)
    if dataset+inc_name in snaps:
        return True
    else:
        return False


def create_lockfile(path):
    """Atomically create a lockfile
       returns file object coresponding to path.
       param path: path to lockfile
       returns: fd of lockfile
       throws: FileExistsError if file exists
       throws: OSerror if file is unable to be created
    """
    try:
        # on linux (the only place this will be used...I hope)
        # according to man 2 open, open with O_CREAT and O_EXCL
        # will fail if the file already exists
        # this gives us an easy atomic lockfile check/create
        return os.open(path, os.O_CREAT | os.O_EXCL)
    except FileExistsError as e:
        # file already exists, another instance must be running
        logging.critical("Error: lock file "+path+" already exists.")
        logging.critical(str(e))
        raise e
    except OSError as e:
        # We're unable to create the file for whatever reason. Report it.
        logging.critical("Error: Unable to create lock file.")
        logging.critical(str(e))
        raise e
    except Exception as e:
        # some other error has occured, report it and exit.
        logging.critical("Error: unable to get open lock file. Error was"+str(e))
        raise e


def clean_lockfile(path, fd):
    """Clean up lockfile
       param path: path to lockfile
       param fd: fd of lockfile
    """
    # close and remove the lockfile.
    try:
        os.close(fd)
        os.remove(path)
    except OSError as e:
        logging.warning("Unable to clean up lockfile.")
        logging.warning(str(e))


def __cleanup_stdout(stdout):
    """Removes empty elements from the stdout/stderr list returned by run
       param stdout: string output of subprocess stdout
       returns: list of lines from stdout
    """
    return list(filter(None, stdout.split('\n')))


class ZFSBackupError(Exception):
    """Exception for this program."""
    # TODO: expand this so it's more than just a message
    def __init__(self, message):
        """Constructor
           param message: message this exception should have
        """
        self.message = message


if sys.version_info[0] != 3 or sys.version_info[1] < 6:
    print("This program requires at least Python 3.6")
    sys.exit("Wrong Python")
if __name__ == "__main__":
    ret = main()
    if ret < 0:
        print("Exited with error. Look into it.")
        sys.exit()
