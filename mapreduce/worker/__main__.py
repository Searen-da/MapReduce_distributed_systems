"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import socket
import threading
import hashlib
import tempfile
import shutil
import heapq
from contextlib import ExitStack
import subprocess as sp
import click
from mapreduce import utils


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # initialize the variables
        self.signals = {"shutdown": False}
        self.threads = []
        self.port = port
        self.host = host
        self.manager_host = manager_host
        self.manager_port = manager_port

        self.start()

    def start(self):
        """Start the Worker."""
        # Create a new TCP socket.

        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock_worker:

            # Bind the TCP socket to the server
            sock_worker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_worker.bind((self.host, self.port))
            sock_worker.listen()

            # register message
            message_dict = {
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            # send register message to manager
            utils.send_message(
                message_dict, self.manager_host, self.manager_port)

            sock_worker.settimeout(1)

            # Receive incoming TCP messages
            while not self.signals["shutdown"]:
                try:
                    manager_socket = sock_worker.accept()[0]
                except socket.timeout:
                    continue
                # receive message str from manager
                # message_str = utils.recv_message(manager_socket)
                # ignore invalid messages
                try:
                    message_dict = json.loads(
                        utils.recv_message(manager_socket))
                except json.JSONDecodeError:
                    continue

                # print(message_dict)
                # process ack message
                if message_dict["message_type"] == "register_ack":
                    # create a new thread for sending heartbeat messages
                    thread_hb = threading.Thread(target=self.udp_sender)
                    self.threads.append(thread_hb)
                    thread_hb.start()
                elif message_dict["message_type"] == "shutdown":
                    self.signals["shutdown"] = True
                elif message_dict["message_type"] == "new_map_task":
                    LOGGER.info('Received task MapTask')
                    self.map_task(message_dict)
                elif message_dict["message_type"] == "new_reduce_task":
                    LOGGER.info('Received task ReduceTask')
                    self.reduce_task(message_dict)

        # shut down all threads
        for thread in self.threads:
            thread.join()

    def udp_sender(self):
        """Send heartbeat messages to manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock_hb:

            # Connect to the UDP socket on server
            sock_hb.connect((self.manager_host, self.manager_port))

            # set timeout for socket
            sock_hb.settimeout(2)

            while not self.signals["shutdown"]:
                # send a heartbeat message to the manager every 2 seconds
                try:
                    heartbeat_dict = {
                        "message_type": "heartbeat",
                        "worker_host": self.host,
                        "worker_port": self.port
                    }
                    message = json.dumps(heartbeat_dict)
                    sock_hb.sendall(message.encode('utf-8'))

                except socket.timeout:
                    continue

                time.sleep(2)

        print("UDP sender shuting down")

    def map_task(self, message_dict):
        """Run a map task."""
        # get the information from the message
        map_task_id = message_dict["task_id"]

        # create a temporary directory for the task
        # prefix = f"mapreduce-local-task{map_task_id:05d}-"
        with tempfile.TemporaryDirectory(
                prefix=f"mapreduce-local-task{map_task_id:05d}-") as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)

            with ExitStack() as stack:
                # create a list of partition files
                partition_files = [
                    stack.enter_context(open(os.path.join(
                        tmpdir, f"maptask{map_task_id:05d}-part{i:05d}"),
                        "a", encoding="utf-8"))
                    for i in range(message_dict["num_partitions"])
                ]

                self.map_partition_write(message_dict, partition_files)

                LOGGER.info("starting to sort the output files")

                # close all partition files, and sort the lines in each file
                for file in partition_files:
                    file.close()
                    # file_path = file.name
                    with open(file.name, "r", encoding="utf-8") as infile:
                        lines = infile.readlines()
                        lines.sort()
                    with open(file.name, "w", encoding="utf-8") as outfile:
                        for line in lines:
                            outfile.write(line)
                    # move the file to the output directory
                    shutil.move(file.name, message_dict["output_directory"])

        # send a 'finished' message to the manager
        message_dict = {
            "message_type": "finished",
            "task_id": map_task_id,
            "worker_host": self.host,
            "worker_port": self.port
        }
        utils.send_message(
            message_dict, self.manager_host, self.manager_port)

    def map_partition_write(self, message_dict, partition_files):
        """Partition the input files and write to partition files."""
        for input_path in message_dict["input_paths"]:
            # Run the map executable on the specified input files
            with open(input_path, encoding="utf-8") as infile:
                with sp.Popen(
                    [message_dict["executable"]],
                    stdin=infile,
                    stdout=sp.PIPE,
                    text=True,
                ) as map_process:
                    for line in map_process.stdout:
                        key = line.split("\t")[0]
                        # Add line to correct partition output file
                        hexdigest = hashlib.md5(
                            key.encode("utf-8")).hexdigest()
                        keyhash = int(hexdigest, base=16)
                        partition_number = keyhash \
                            % message_dict["num_partitions"]
                        # append the line to partition file
                        partition_files[partition_number].write(line)

    def reduce_task(self, message_dict):
        """Run a reduce task."""
        # get the information from the message
        reduce_task_id = message_dict["task_id"]
        reduce_input_paths = message_dict["input_paths"]
        reduce_executable = message_dict["executable"]
        reduce_output_directory = message_dict["output_directory"]

        # merge the input files using heapq.merge()
        # instream = heapq.merge(*[open(file) for file in reduce_input_paths])

        with ExitStack() as stack:
            input_files = [stack.enter_context(
                open(file, encoding="utf-8")) for file in reduce_input_paths]
            instream = heapq.merge(*input_files)

            prefix = f"mapreduce-local-task{reduce_task_id:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)
                with open(os.path.join(
                        tmpdir, f"part-{reduce_task_id:05d}"),
                        "w", encoding="utf-8") as outfile:
                    with sp.Popen(
                        [reduce_executable],
                        text=True,
                        stdin=sp.PIPE,
                        stdout=outfile,
                    ) as reduce_process:
                        # Pipe input to reduce_process
                        for line in instream:
                            reduce_process.stdin.write(line)
                        reduce_process.stdin.close()

                # move the file to the output directory
                shutil.move(os.path.join(
                    tmpdir, f"part-{reduce_task_id:05d}"),
                    reduce_output_directory)

                # send a message to the manager
                message_dict = {
                    "message_type": "finished",
                    "task_id": reduce_task_id,
                    "worker_host": self.host,
                    "worker_port": self.port
                }
                utils.send_message(
                    message_dict, self.manager_host, self.manager_port)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    handler = utils.main_duplicate(logfile)
    formatter = logging.Formatter(
        f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
