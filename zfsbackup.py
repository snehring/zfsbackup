"""
zfsbackup.py a simple zfs backup utility
"""

import argparse
import logging
import subprocess
from subprocess import CalledProcessError, TimeoutExpired
import multiprocessing
from multiprocessing.pool import ThreadPool
import re
import os
import sys
from datetime import datetime
import time
import random
import yaml
import shutil


def main():
    # TODO: argparse setup
    ap_desc = """Program to automatically create and send snapshots of zfs
                 datasets."""
    arg_parser = argparse.ArgumentParser(description=ap_desc)
    arg_parser.add_argument(
        "-c", "--config", help="path to configuration for %(prog)s", type=str
    )
    arg_parser.add_argument(
        "dataset", type=str, nargs="?", help="name of dataset to replicate"
    )
    arg_parser.add_argument(
        "destination", type=str, nargs="?", help="where to send the dataset"
    )
    arg_parser.add_argument(
        "transport",
        type=str,
        nargs="?",
        default="local",
        help="how to send the dataset, local or ssh. "
        + "If not provided, local assumed."
        + "ssh format: "
        + "ssh:username@hostname<:port>",
    )
    args = arg_parser.parse_args()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # hard coded if you don't provide one in the config file, sorry.
    lf_path = "/var/lock/zfsbackup.lock"
    # TODO: make this user customizable
    incremental_name = "@zfsbackup-last"
    parallel_sends = 2
    # error counter
    errors = 0
    if args.dataset or args.destination:
        # single dataset run
        if not args.dataset and args.destination:
            logger.error("Please provide both a dataset and a destination")
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
        dests = [{"dest": dest, "transport": transport}]
        try:
            stragglers = has_stragglers(name)
        except ZFSBackupError:
            logger.warning(
                "Unable to get list of existing snapshots for dataset: %s. IT WAS NOT BACKED UP!",
                name,
            )
            errors += 1

        if stragglers:
            logger.warning(
                "Dataset: %s has left over temporary snapshots. IT WAS NOT BACKED UP! You need to resolve this manually. Make sure everything is consistent and then remove/rename the temporary zfsbackup-yyyymmdd-hhmm snaps.",
                name,
            )
            return -1
        else:
            try:
                backup_dataset(name, dests, incremental_name)
            except ZFSBackupError:
                logger.warning(
                    "Dataset backup of %s to %s FAILED! YOU'LL WANT TO SEE TO THAT!",
                    name,
                    dest,
                )
                errors += 1
    elif args.config:
        # config run
        if not os.path.exists(args.config):
            logger.error("Exiting: Cannot find config file at %s", args.config)
            return -1
        conf = validate_config(args.config)
        if conf.get("log_file"):
            # set log file
            # if the path is invalid or not writable I bet this'll complain
            # and that's fine, I'd percolate up any exception anyway
            logging.basicConfig(
                filename=conf.get("log_file"),
                filemode="a",
                encoding="UTF-8",
                format="%(asctime)s (%(levelname)s) %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
                level=logging.INFO,
            )
        if conf.get("lock_file"):
            lf_path = conf.get("lock_file")
        retain_snaps = conf.get("retain_snaps")
        # create lockfile
        try:
            lf_fd = create_lockfile(lf_path)
        except Exception:
            logger.critical("Exiting: cannot get a lockfile.")
            return -1
        results = parallel_process_backup(
            parallel_sends, conf.get("datasets"), incremental_name, retain_snaps
        )

    elif not args.config:
        # config file not provided
        logger.error("Config file required if no other arguments given.")
        return -1
    else:
        # we shouldn't ever get here
        logger.error("Woops, I guess I broke argument parsing")
        return -128

    # TODO: determine if we want a 'retry queue' of failed datasets
    # if so, make sure those are added into the failure queue above
    clean_lockfile(lf_path, lf_fd)
    for i in results:
        if i:
            return -10
    return 0


def parallel_process_backup(
    parallel_sends: int, datasets: list, incremental_name: str, retain_snaps
):
    with ThreadPool(processes=parallel_sends) as pool:
        args = []
        for ds in datasets:
            args.append((ds, incremental_name, retain_snaps))
        results = pool.starmap(do_parallel_send, args, chunksize=1)
        return results


def do_parallel_send(dataset: dict, incremental_name: str, retain_snaps: int):
    # sleep randomly before we get started
    time.sleep(random.randrange(1, 16))
    logger = logging.getLogger(__name__)

    name = dataset.get("dataset_name")
    try:
        stragglers = has_stragglers(name)
    except ZFSBackupError as e:
        logger.error(
            "Unable to get list of existing snaps for dataset %s. IT WAS NOT BACKED UP!",
            name,
        )
        return e
    if stragglers:
        logger.error(
            "Dataset: %s has straggler snapshots. IT WAS NOT BACKED UP! Remove zfsbackup-yyyymmdd-hhmm snaps for this dataset.",
            name,
        )
        return ZFSBackupError("dataset has stragglers.")
    try:
        backup_dataset(name, dataset.get("destinations"), incremental_name)
        # Delete old snaps
        clean_dest_snaps(dataset.get("destinations"), retain_snaps)
    except ZFSBackupError as e:
        logger.error(
            "Dataset backup of %s to %s FAILED! YOU'LL WANT TO SEE TO THAT!",
            name,
            dataset.get("destinations"),
        )
        return e
    return False


def validate_config(conf_path):
    """Peforms basic validation of config file format.
    I hope for your sake the actual dataset and destination paths
    are correct.
    :param conf_path: path to the config file
    :return: returns the validated yaml file as a python object"""
    logger = logging.getLogger(__name__)
    conf_stat = os.stat(conf_path)
    # could open this up to only deny writable by others/group, but eh.
    # I was going to enforce the file being owned by root, but that's a bit
    # too restrictive probably. Verifying that it's owned by who's executing
    # is probably sufficient.
    if not (
        ((conf_stat.st_mode & 0o677) == 0o600) and (conf_stat.st_uid == os.geteuid())
    ):
        # perms incorrect for config file
        raise ZFSBackupError(
            "Config file has incorrect permissions. "
            + "Must be 600 and owned by the user "
            + "running the program."
        )
    with open(conf_path, encoding="UTF-8") as conf_f:
        try:
            conf = yaml.safe_load(conf_f.read())
        except yaml.YAMLError as e:
            # parsing error
            logger.error("Invalid config file.")
            raise e
    if not conf.get("datasets"):
        raise ZFSBackupError("No datasets defined, or defined incorrectly.")
    for d in conf.get("datasets"):
        if not d or not d.get("dataset_name") or not d.get("destinations"):
            raise ZFSBackupError("Dataset config incorrectly defined.")
        for e in d.get("destinations"):
            if (not e) or (not e.get("dest")) or (not e.get("transport")):
                raise ZFSBackupError(
                    f"Destination config defined incorrectly for {e.get('dataset_name')}"
                )
    return conf


def backup_dataset(dataset, destinations, inc_snap):
    """Backup a dataset to the specified destinations using the specified
    transport. If it is determined that this is an incremental backup
    it will do an incremental send and delete the old inc_snap and
    rename the most recent snapshot to inc_snap.
    Otherwise it will create a snap, send it, and rename it to
    inc_snap when finished.
    param dataset: dataset to be backed up
    param destinations: list of dest dicts
    param inc_snap: the incremental source snapshot
    raises: ZFSBackupError"""
    logger = logging.getLogger(__name__)
    try:
        for d in destinations:
            transport = d.get("transport")
            if get_transport_type(transport) == "ssh":
                # if we're doing ssh and the connection fails abort to avoid nuisance snapshot cleanup.
                username, hostname, port = parse_ssh_transport(transport)
                try:
                    __run_ssh_command(username, hostname, port, ["zfs", "--version"])
                except CalledProcessError as e:
                    raise ZFSBackupError(
                        f"Test connection to {transport} failed. Aborting."
                    ) from e
                except TimeoutExpired as e:
                    raise ZFSBackupError(
                        f"Test connection to {transport} timed out. Aborting."
                    ) from e
        new_snap = create_timestamp_snap(dataset)
        if has_backuplast(dataset, inc_snap):
            errors = 0
            # do incremental
            for d in destinations:
                destination = d.get("dest")
                transport = d.get("transport")
                logger.info(
                    "Beginning incremental send of %s to %s via %s.",
                    dataset,
                    destination,
                    transport,
                )
                try:
                    send_incremental(
                        dataset + inc_snap,
                        dataset + new_snap,
                        destination,
                        transport=transport,
                    )
                    logger.info(
                        "Incremental send of %s to %s via %s finished.",
                        dataset + new_snap,
                        destination,
                        transport,
                    )
                except ZFSBackupError:
                    errors += 1
                    continue
                if verify_backup(new_snap, destination, transport):
                    # good backup
                    logger.info(
                        "Verification of %s via %s succeeded.",
                        destination + new_snap,
                        transport,
                    )
                else:
                    # verify failed for whatever reason
                    logger.warning(
                        "Snapshot verification failed for %s at %s via %s.",
                        dataset + new_snap,
                        destination,
                        transport,
                    )
                    errors += 1
            if errors > 0:
                raise ZFSBackupError(
                    f"Errors were encountred while backing up {dataset + new_snap}. Please check the logs."
                )
            # delete old incremental marker
            try:
                delete_snapshot(dataset + inc_snap)
                logger.info("Deleted %s.", dataset+inc_snap)
            except ZFSBackupError:
                logger.error(
                    "Unable to delete %s. YOU WILL NEED TO DELETE IT AND THEN RENAME %s TO %s",
                    dataset + inc_snap,
                    dataset + new_snap,
                    dataset + inc_snap,
                )
        else:
            # do full send
            errors = 0
            for d in destinations:
                destination = d.get("dest")
                transport = d.get("transport")
                try:
                    logger.info(
                        "Beginning full send of %s to %s via %s.",
                        dataset + new_snap,
                        destination,
                        transport,
                    )
                    send_full(dataset + new_snap, destination, transport=transport)
                    logger.info(
                        "Full send of %s to %s via %s finished.",
                        dataset + new_snap,
                        destination,
                        transport,
                    )
                except ZFSBackupError:
                    errors += 1
                    continue
                if verify_backup(new_snap, destination, transport):
                    # good backup
                    logger.info(
                        "Verification of %s via %s succeeded.",
                        destination + new_snap,
                        transport,
                    )
                else:
                    # verify failed
                    logger.warning(
                        "Snapshot verification failed for %s at %s via %s.",
                        dataset + new_snap,
                        destination,
                        transport,
                    )
                    errors += 1
            if errors > 0:
                raise ZFSBackupError(
                    f"Errors were encountered while backing up {dataset + new_snap}. Please check the logs."
                )
        # rename dataset+new_snap to dataset+inc_snap
        try:
            rename_snapshot(dataset + new_snap, dataset + inc_snap)
            logger.info(
                "Rename of %s to %s finished.", dataset + new_snap, dataset + inc_snap
            )
        # done
        except ZFSBackupError as e:
            logger.error(
                "UNABLE TO RENAME %s TO %s. YOU WILL NEED TO DO THIS MANUALLY!",
                dataset + new_snap,
                dataset + inc_snap,
            )
            raise e
    except ZFSBackupError as e:
        logger.error("Failed backup of %s to %s", dataset, destinations)
        raise e


def verify_backup(snapshot, destination, transport):
    """Verify backup is at destination
    param snapshot: snapshot that needs its presence verified (@name)
    param destination: where snapshot should be (dataset)
    param transport: how to get to destination
    returns: True if the snapshot is present at destination, else False
    """
    logger = logging.getLogger(__name__)
    try:
        if get_transport_type(transport) == "local":
            zfs_command = [
                shutil.which("zfs"),
                "list",
                "-H",
                "-t",
                "snapshot",
                "-o",
                "name",
                destination + snapshot,
            ]
            subprocess.run(
                zfs_command,
                check=True,
                timeout=600,
                encoding="utf-8",
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            return True
        elif get_transport_type(transport) == "ssh":
            # TODO: make the ssh communication it's own function probably
            username, hostname, port = parse_ssh_transport(transport)
            zfs = f"zfs list -H -t snapshot -o name {destination}{snapshot}"
            ssh_command = [
                shutil.which("ssh"),
                "-o",
                "PreferredAuthentications=publickey",
                "-o",
                "PubkeyAuthentication=yes",
                "-o",
                "StrictHostKeyChecking=yes",
                "-p",
                port,
                "-l",
                username,
                hostname,
                zfs,
            ]
            subprocess.run(
                ssh_command,
                check=True,
                timeout=600,
                encoding="utf-8",
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            return True
        else:
            # crap we don't do
            return False
    except Exception as e:
        logger.debug(
            "Unable to verify snap: %s exists at %s via %s. Exception was %s",
            snapshot,
            destination,
            transport,
            e,
        )
        return False


def create_snapshot(dataset, name):
    """Create a snapshot of the given dataset with the specified name
    param dataset: dataset to snapshot
    param name: name of snapshot, sans '@'
    throws: ZFSBackupError if snapshot fails
    """
    try:
        subprocess.run(
            [shutil.which("zfs"), "snap", f"{dataset}@{name}"],
            timeout=600,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
            encoding="utf-8",
        )
    except CalledProcessError as e:
        # returned non-zero
        raise ZFSBackupError(
            f"Failed to create snapshot {dataset}@{name}. Got: {str(__cleanup_stdout(e.stderr))}"
        ) from e
    except TimeoutExpired as e:
        # timed out
        raise ZFSBackupError(
            f"Failed to create snapshot {dataset}@{name}. Timeout reached."
        ) from e


def create_timestamp_snap(dataset):
    """Create a snapshot with the zfsbackup-YYYYMMDD-HHMM name format.
    returns name of created snapshot
    param dataset: dataset to create a timestamp snap of
    returns: string representing name of snapshot created
    throws: ZFSBackupError if snapshot fails
    """
    # call create_snapshot with correct name
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    create_snapshot(dataset, "zfsbackup-" + timestamp)
    return "@zfsbackup-" + timestamp


def delete_snapshot(snapshot):
    """delete snapshot specified by snapshot.
    specified name should literally be the name returned by
    zfs list -t snap
    param snapshot: snapshot to remove (dataset@name)
    throws ZFSBackupError if snapshot delete fails
    """
    logger = logging.getLogger(__name__)
    # try to make sure we're not deleting anything other than a snapshot
    if "@" not in snapshot:
        raise ZFSBackupError(
            f"Tried to delete something other than a snapshot. Was: {snapshot}"
        )
    try:
        subprocess.run(
            [shutil.which("zfs"), "destroy", snapshot],
            timeout=600,
            stderr=subprocess.PIPE,
            check=True,
            encoding="utf-8",
        )
    except CalledProcessError as e:
        # returned non-zero
        logger.error("Unable to destroy snapshot %s", snapshot)
        logger.error("Got: %s", __cleanup_stdout(e.stderr))
        raise ZFSBackupError(f"Failed to delete snapshot {snapshot}") from e
    except TimeoutExpired as e:
        # timed out
        raise ZFSBackupError(
            f"Unable to destroy snapshot {snapshot}. Timeout reached."
        ) from e


def rename_dataset(dataset, newname):
    """Renames a dataset to newname
    param dataset: dataset to be renamed
    param newname: new name of dataset
    throws: ZFSBackupError if rename fails
    """
    logger = logging.getLogger(__name__)
    try:
        subprocess.run(
            [shutil.which("zfs"), "rename", dataset, newname],
            stderr=subprocess.PIPE,
            check=True,
            timeout=600,
            encoding="utf-8",
        )
    except CalledProcessError as e:
        # command returned non-zero error code
        logger.error("Unable to rename dataset %s to %s", dataset, newname)
        logger.error("Got: %s", __cleanup_stdout(e.stderr))
        raise ZFSBackupError(
            f"Failed to rename dataset: {dataset} newname: {newname}"
        ) from e
    except TimeoutExpired as e:
        # timed out
        raise ZFSBackupError(
            f"Unable to rename dataset {dataset}. Timeout Reached."
        ) from e


def rename_snapshot(snapshot, newname):
    """Renames a snapshot to newname
    param snapshot: snapshot to be renamed
    param newname: new name of snapshot
    throws: ZFSBackupError if rename fails or if snapshot isn't a snapshot
    """
    # check that it's a snapshot
    if ("@" not in snapshot) or ("@" not in newname):
        raise ZFSBackupError(
            f"Tried to rename a non-snapshot or rename a snapshot to a non-snapshot. Snapshot was: {snapshot} and newname was: {newname}"
        )
    # call the function to actually rename
    rename_dataset(snapshot, newname)


def send_snapshot(snapshot, destination, transport="local", incremental_source=None):
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
    logger = logging.getLogger(__name__)
    send_flags = ""
    recv_flags = "-o canmount=noauto"
    if is_encrypted_dataset(snapshot):
        send_flags = "-w"
        recv_flags = "-o canmount=noauto"
    elif get_transport_type(transport) == "ssh":
        send_flags = ""

    if "@" not in snapshot:
        raise ZFSBackupError(f"tried to send non snapshot {snapshot}")

    zsend_command = [shutil.which("zfs"), "send"]

    if incremental_source:
        if "@" not in incremental_source:
            raise ZFSBackupError(
                f"Incremental source is not a snapshot. snap: {snapshot} dest {destination} incremental_source {incremental_source}"
            )
        if not send_flags:
            zsend_command.extend(["-i", incremental_source, snapshot])
        else:
            zsend_command.extend(
                [
                    send_flags,
                    "-i",
                    incremental_source,
                    snapshot,
                ]
            )
    else:
        if not send_flags:
            zsend_command.extend([snapshot])
        else:
            zsend_command.extend([send_flags, snapshot])

    zrecv_command = [shutil.which("zfs"), "recv", recv_flags, destination]
    if get_transport_type(transport) == "local":
        logger.debug("Beginning local send of %s to %s", snapshot, destination)
        with subprocess.Popen(
            zsend_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as zfs_send:
            with subprocess.Popen(
                zrecv_command, stdin=zfs_send.stdout, stderr=subprocess.PIPE
            ) as zfs_recv:
                try:
                    zfs_recv.communicate()
                    zfs_recv.wait()
                    if zfs_recv.returncode != 0:
                        logger.debug("zfs recv returned error.")
                        zfs_send.kill()
                        zfs_send.wait()
                        raise ZFSBackupError(
                            "zfs recv of "
                            + snapshot
                            + " to "
                            + destination
                            + " failed."
                        )
                    zfs_send.wait()
                    if zfs_send.returncode != 0:
                        raise ZFSBackupError(
                            f"zfs send of {snapshot} to {destination} failed."
                        )
                except Exception as e:
                    raise ZFSBackupError(
                        f"Caught an exception while sending {snapshot}."
                    ) from e
                logger.debug(
                    "Finished local send of %s to %s.",
                    snapshot,
                    destination,
                )

    elif get_transport_type(transport) == "ssh":
        username, hostname, port = parse_ssh_transport(transport)
        with subprocess.Popen(
            zsend_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as zfs_send:
            # TODO: have a configurable for ssh-key instead of just assuming
            ssh_remote_command = f"mbuffer -m 1G 2> /dev/null | lz4 -d | zfs recv {recv_flags} {destination}"
            ssh_command = [
                shutil.which("ssh"),
                "-o",
                "PreferredAuthentications=publickey",
                "-o",
                "PubkeyAuthentication=yes",
                "-o",
                "StrictHostKeyChecking=yes",
                "-p",
                port,
                "-l",
                username,
                hostname,
                ssh_remote_command,
            ]
            with subprocess.Popen(
                [shutil.which("mbuffer"), "-m", "1G"],
                stdin=zfs_send.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            ) as mbuffer:
                with subprocess.Popen(
                    [shutil.which("lz4")],
                    stdin=mbuffer.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                ) as lz4:
                    with subprocess.Popen(
                        ssh_command,
                        stdin=lz4.stdout,
                        stderr=subprocess.PIPE,
                    ) as ssh_recv:
                        try:
                            ssh_recv.wait()
                            if ssh_recv.returncode != 0:
                                logger.debug("ssh recv command returned error")
                                lz4.kill()
                                lz4.wait()
                                mbuffer.kill()
                                mbuffer.wait()
                                zfs_send.kill()
                                zfs_send.wait()
                                raise ZFSBackupError(
                                    f"ssh send of {snapshot} to {destination} failed. ssh recv pipe errors: {__cleanup_stdout(ssh_recv.stderr.read().decode('UTF-8'))}"
                                )
                            lz4.wait()
                            if lz4.returncode != 0:
                                mbuffer.kill()
                                mbuffer.wait()
                                zfs_send.kill()
                                zfs_send.wait()
                                raise ZFSBackupError(
                                    f"ssh send of {snapshot} to {destination} failed. lz4 errors: {__cleanup_stdout(lz4.stderr.read().decode('UTF-8'))}"
                                )
                            mbuffer.wait()
                            if mbuffer.returncode != 0:
                                zfs_send.kill()
                                zfs_send.wait()
                                raise ZFSBackupError(
                                    f"ssh send of {snapshot} to {destination} failed. mbuffer error."
                                )
                            zfs_send.wait()
                            if zfs_send.returncode != 0:
                                raise ZFSBackupError(
                                    f"ssh send of {snapshot} to {destination} failed. zfs send errors: {__cleanup_stdout(zfs_send.stderr.read().decode('UTF-8'))}"
                                )

                        except (CalledProcessError, OSError) as e:
                            raise ZFSBackupError(
                                f"Caught an exception while sending: {str(e)}"
                            ) from e

    else:
        # some transport we don't support
        # shouldn't happen with config parsing
        # handle it anyway
        raise ZFSBackupError(f"Invalid transport: {transport}.")


def send_full(snapshot, destination, transport="local"):
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


def send_incremental(snapshot1, snapshot2, destination, transport="local"):
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
    send_snapshot(
        snapshot2, destination, transport=transport, incremental_source=snapshot1
    )


def has_stragglers(dataset):
    """Returns true if dataset has straggler zfsbackup-<datestamp> snapshots
    param dataset: dataset to check
    returns: True if stragglers are found, False otherwise
    throws: ZFSBackupError if unable to get list of snapshots
    """
    snaps = get_snapshots(dataset)
    regex = re.compile(r".*@zfsbackup-\d{8}-\d{6}")
    # this is likely not the best way to do this, but it shouldn't be too awful
    matches = list(filter(regex.match, snaps))
    return bool(matches)


def get_snapshots(dataset):
    """returns a python list of snapshots for a dataset
    param dataset: dataset to enumerate snapshots for
    returns: list of snapshots
    throws: ZFSBackupError if unable to get list of snapshots
    """
    logger = logging.getLogger(__name__)
    # get list of snapshots
    try:
        zfs_command = [
            shutil.which("zfs"),
            "list",
            "-H",
            "-t",
            "snapshot",
            "-d",
            "1",
            "-o",
            "name",
            dataset,
        ]
        zfs = subprocess.run(
            zfs_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            timeout=600,
            encoding="utf-8",
        )
        # remove empty lines and return a list with the contents of stdout
        snaps = __cleanup_stdout(zfs.stdout)
        return snaps
    except CalledProcessError as e:
        # command returned non-zero error code
        logger.debug(
            "Unable to get list of snapshots for %s. zfs list returned non-zero return code.",
            dataset,
        )
        logger.debug("Got: %s", __cleanup_stdout(e.stderr))
        raise ZFSBackupError(f"Unable to get list of snapshots for {dataset}.") from e
    except TimeoutExpired as e:
        # command timed out
        raise ZFSBackupError(
            f"Unable to get list of snapshots for {dataset}. Timeout reached."
        ) from e


def is_encrypted_dataset(dataset):
    """Returns true if the dataset is encrypted, false otherwise
    param dataset: dataset to check
    returns: true if encrypted, false otherwise
    throws ZFSBackupError if it can't figure it out
    """
    logger = logging.getLogger(__name__)
    try:
        zfs_command = [
            shutil.which("zfs"),
            "get",
            "-H",
            "-d",
            "0",
            "encryption",
            dataset,
        ]
        results = __run_command(zfs_command)[0].split("\t")
        return (
            results[0] == dataset and results[1] == "encryption" and results[2] != "off"
        )
    except CalledProcessError as e:
        logger.debug("Unable to determine if dataset %s is encrypted or not.", dataset)
        logger.debug("Got: %s", __cleanup_stdout(e.stderr))
        raise ZFSBackupError(
            f"Unable to determine encryption status for {dataset}"
        ) from e
    except TimeoutExpired as e:
        raise ZFSBackupError(
            f"Unable to get encryption status for {dataset}. Timeout reached."
        ) from e


def has_backuplast(dataset, inc_name):
    """return true if dataset has a backup-last snapshot
    param dataset: dataset to check
    param inc_name: name of snapshot that is the last backup. Include '@'
    returns: True if the snapshot is found, False otherwise
    throws: ZFSBackupError if a list of snapshots cannot be obtained
    """
    snaps = get_snapshots(dataset)
    if dataset + inc_name in snaps:
        return True
    else:
        return False


def clean_dest_snaps(destinations, global_retain_snaps=None):
    """
    delete all but the n snapshots from destinations per config
    param destinations: list of destinations from config file
    param global_retain_snaps: number of snapshots that should be kept
    as defined by the retain_snaps global config param.
    """
    logger = logging.getLogger(__name__)
    for dest in destinations:
        dataset = dest.get("dest")
        transport = dest.get("transport")
        if dest.get("retain_snaps") is None and global_retain_snaps is None:
            # We're not deleting anything
            logger.info("Not cleaning up snaps for: %s via %s.", dataset, transport)
            return
        elif dest.get("retain_snaps") is None:
            num_snaps = global_retain_snaps
        else:
            num_snaps = dest.get("retain_snaps")
        zfs_command = [
            "zfs",
            "list",
            "-H",
            "-t",
            "snapshot",
            "-d",
            "1",
            "-o",
            "name",
            dataset,
        ]
        if get_transport_type(transport) == "local":
            # local transport
            try:
                snaps = __snap_delete_format(__run_command(zfs_command), num_snaps)
            except subprocess.SubprocessError:
                logger.warning(
                    "Unable to get list of snapshots to delete from %s via %s. Aborting deletion.",
                    dataset,
                    transport,
                )
                return
            errors = 0
            logger.info("Deleting %s from %s via %s", len(snaps), dataset, transport)
            for snap in snaps:
                try:
                    delete_snapshot(snap)
                except ZFSBackupError:
                    errors += 1
            if errors > 0:
                logger.warning(
                    "Encountered errors while deleting old snapshots from destination: %s via %s.",
                    dataset,
                    transport,
                )
        elif get_transport_type(transport) == "ssh":
            # ssh transport
            user, host, port = parse_ssh_transport(transport)
            try:
                snaps = __snap_delete_format(
                    __run_ssh_command(user, host, port, zfs_command), num_snaps
                )
            except subprocess.SubprocessError:
                logger.warning(
                    "Unable to get list of snapshots to delete from %s via %s. Aborting deletion.",
                    dataset,
                    transport,
                )
                return
            errors = 0
            logger.info("Deleting %s from %s via %s.", len(snaps), dataset, transport)
            for snap in snaps:
                # this will be executed on the remote host, assume the shell will be able to work with the unqualified path
                zfs_snap_delete = ["zfs", "destroy", snap]
                try:
                    __run_ssh_command(user, host, port, zfs_snap_delete)
                except subprocess.SubprocessError:
                    errors += 1
            if errors > 0:
                logger.warning(
                    "Encountered errors while deleting old snapshots from destination: %s via %s.",
                    dataset,
                    transport,
                )
        else:
            # unsupported transport
            raise ZFSBackupError(f"Invalid transport: {transport}.")


def __snap_delete_format(snaps, nsave):
    """
    sort the list of snaps and pair down to those we want to delete
    filters the list for the snap format we have
    param snaps: list of snaps
    param nsave: number of snaps to save
    """
    regex = re.compile(r".*@zfsbackup-\d{8}-\d{6}")
    matches = list(filter(regex.match, snaps))
    if len(matches) < nsave:
        return []
    return sorted(matches)[: len(matches) - nsave]


def __run_command(command):
    """
    run a command
    param command: command to run
    returns: the stdout returned from command as a list
    """
    cmd = subprocess.run(
        command, stdout=subprocess.PIPE, check=True, encoding="utf8", timeout=600
    )
    return __cleanup_stdout(cmd.stdout)


def __run_ssh_command(user, host, port, cmd):
    """
    do a command via ssh
    param user: username to run as
    param host: host to run on
    param ssh_args: arguments to ssh
    param cmd: command to run
    returns: the stdout of the command
    """
    ssh_inv = [
        shutil.which("ssh"),
        "-o",
        "PreferredAuthentications=publickey",
        "-o",
        "PubkeyAuthentication=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-p",
        port,
        "-l",
        user,
        host,
        " ".join(cmd),
    ]
    return __run_command(ssh_inv)


def create_lockfile(path):
    """Atomically create a lockfile
    returns file object coresponding to path.
    param path: path to lockfile
    returns: fd of lockfile
    throws: FileExistsError if file exists
    throws: OSerror if file is unable to be created
    """
    logger = logging.getLogger(__name__)
    try:
        # on linux (the only place this will be used...I hope)
        # according to man 2 open, open with O_CREAT and O_EXCL
        # will fail if the file already exists
        # this gives us an easy atomic lockfile check/create
        return os.open(path, os.O_CREAT | os.O_EXCL, mode=600)
    except FileExistsError as e:
        # file already exists, another instance must be running
        logger.critical("Lock file %s already exists.", path)
        raise e
    except OSError as e:
        # We're unable to create the file for whatever reason. Report it.
        logger.critical("Unable to create lock file. %s", e)
        raise e
    except Exception as e:
        # some other error has occured, report it and exit.
        logger.critical("Unable to open lock file. Error was %s", e)
        raise e


def clean_lockfile(path, fd):
    """Clean up lockfile
    param path: path to lockfile
    param fd: fd of lockfile
    """
    logger = logging.getLogger(__name__)
    # close and remove the lockfile.
    try:
        os.close(fd)
        os.remove(path)
    except OSError as e:
        logger.warning("Unable to clean up lockfile.")
        logger.warning(str(e))


def __cleanup_stdout(stdout):
    """Removes empty elements from the stdout/stderr list returned by run
    param stdout: string output of subprocess stdout
    returns: list of lines from stdout
    """
    if stdout is None:
        return ["No output"]
    else:
        return list(filter(None, stdout.split("\n")))


def get_transport_type(transport):
    return (
        transport.lower()
        if transport.lower() == "local"
        else transport.lower().split(":")[0]
    )


def parse_ssh_transport(transport):
    """
    Parse an ssh transport for user, host and port
    param transport: ssh transport string
    returns: list of user, host, and port
    """
    user, host = transport.lower().split(":")[1].split("@")
    if len(transport.split(":")) > 2:
        # 3rd element is port
        port = transport.lower().split(":")[2]
    else:
        port = "22"
    return [user, host, port]


class ZFSBackupError(Exception):
    """Exception for this program."""

    # TODO: expand this so it's more than just a message
    def __init__(self, message):
        logger = logging.getLogger(__name__)
        """Constructor
        param message: message this exception should have
        """
        self.message = message

        logger.error(message)


if sys.version_info[0] != 3 or sys.version_info[1] < 6:
    print("This program requires at least Python 3.6")
    sys.exit("Wrong Python")
if __name__ == "__main__":
    ret = main()
    if ret < 0:
        print("Exited with error. Look into it.")
        sys.exit()
