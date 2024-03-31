"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import threading
import socket
import shutil
from pathlib import Path
from collections import OrderedDict
import click
from mapreduce import utils


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # initialize the variables
        self.signals = {"shutdown": False}
        # self.threads = []
        # self.port = port
        # self.host = host
        self.worker_list = []
        self.job_queue = []
        # self.job_id = 0
        self.num_jobs_total = 0
        self.mapping_tasks = OrderedDict()
        self.reducing_tasks = OrderedDict()
        self.num_task_finished = 0

        self.launch(host, port)   # launch the manager

    def launch(self, host, port):
        """Launch the Manager node."""
        threads = []
        thread_udp = threading.Thread(
            target=self.udp_listener, args=(host, port))
        thread_ftl = threading.Thread(target=self.fault_tolerance)

        threads.append(thread_udp)
        threads.append(thread_ftl)
        thread_udp.start()
        print("UDP listener started")
        thread_ftl.start()
        print("fault tolerance started")

        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()

            job_thread = threading.Thread(target=self.run_job)
            threads.append(job_thread)
            job_thread.start()
            print("job thread started")

            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)

            while not self.signals["shutdown"]:

                # Wait for a connection for 1s.
                # The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    worker_socket = sock.accept()[0]
                except socket.timeout:
                    continue
                # receive message from worker
                # message_str = utils.recv_message(worker_socket)
                try:
                    message_dict = json.loads(
                        utils.recv_message(worker_socket))
                except json.JSONDecodeError:
                    continue

                if message_dict["message_type"] == "register":
                    # IMPLEMENT ME
                    # worker_host = message_dict["worker_host"]
                    # worker_port = message_dict["worker_port"]
                    # build connection with worker
                    self.register_worker(
                        message_dict["worker_host"],
                        message_dict["worker_port"])

                elif message_dict["message_type"] == "shutdown":
                    self.signals["shutdown"] = True
                    # forward the shutdown message to the workers
                    # that are still alive and has registered
                    self.shutdown_worker()

                elif message_dict["message_type"] == "new_manager_job":
                    self.add_job(message_dict)

                elif message_dict["message_type"] == "finished":
                    # find the worker in the worker list
                    for i, worker in enumerate(self.worker_list):
                        if worker["worker_host"] \
                            == message_dict["worker_host"] \
                            and worker["worker_port"] \
                                == message_dict["worker_port"]:
                            self.worker_list[i]["state"] = "ready"
                            self.worker_list[i]["task_id"] = None
                            LOGGER.info("Worker %s:%s finised task %s",
                                        worker["worker_host"],
                                        worker["worker_port"],
                                        message_dict["task_id"])
                            break
                    # update the task status
                    self.num_task_finished += 1

            print("TCP listener shutting down")

        # join the threads
        for thread in threads:
            thread.join()
        print("manager shutting down")

    def udp_listener(self, host, port):
        """Check for dead workers, receiving heartbeats, this is UDP."""
        # Create an INET, STREAMing socket, this is UDP.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(1)

            # No sock.listen() since UDP doesn't establish connections like TCP

            # Receive incoming UDP messages
            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                # ignore invalid messages
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)  # for debugging
                if message_dict["message_type"] == "heartbeat":
                    # find the worker
                    for i, worker in enumerate(self.worker_list):
                        if worker["worker_host"] \
                            == message_dict["worker_host"] and \
                                worker["worker_port"] \
                                == message_dict["worker_port"]:
                            self.worker_list[i]["last_hb"] = time.time()

            print("udp listener shutting down")

    def fault_tolerance(self):
        """Check the health of all workers."""
        while not self.signals["shutdown"]:
            # check the health of all workers
            for i, worker in enumerate(self.worker_list):
                print(worker["worker_port"], worker["state"],
                      time.time() - worker["last_hb"])
                if worker["state"] != "dead" and \
                        time.time() - worker["last_hb"] > 10:
                    # mark the worker as dead
                    self.worker_list[i]["state"] = "dead"
                    print("Worker %s:%s is dead",
                          worker["worker_host"], worker["worker_port"])
            time.sleep(1)

    def register_worker(self, worker_host, worker_port):
        """Register a worker with the manager."""
        # send ack message to the worker
        message_dict = {
            "message_type": "register_ack",
            "worker_host": worker_host,
            "worker_port": worker_port,
        }

        LOGGER.info("Registering worker %s:%s", worker_host, worker_port)

        ack_success = utils.send_message(
            message_dict, worker_host, worker_port)

        # check if the worker is already in the worker list
        for i, worker in enumerate(self.worker_list):
            # if the worker is re-registering
            if worker["worker_host"] == worker_host \
                    and worker["worker_port"] == worker_port:
                # if ack message is sent successfully
                if ack_success:
                    # update the last heartbeat time
                    self.worker_list[i]["last_hb"] = time.time()
                    # the worker did not finish the task,
                    # but it is indeed dead and re-registering
                    if worker["task_id"] is not None:
                        self.worker_list[i]["state"] = "revive"
                    # a free worker re-registering
                    else:
                        worker["state"] = "ready"
                # ack message is not sent successfully
                else:
                    self.worker_list[i]["state"] = "dead"

                return

        # if this is new registration
        new_worker = {
            "worker_host": worker_host,
            "worker_port": worker_port,
            "state": "ready",
            "task_id": None,
            "last_hb": time.time(),
        }

        if not ack_success:
            new_worker["state"] = "dead"

        self.worker_list.append(new_worker)

    def shutdown_worker(self):
        """Shutdown all workers."""
        for worker in self.worker_list:
            # forward the message to all living workers
            if worker["state"] == "dead":
                continue

            worker_host = worker["worker_host"]
            worker_port = worker["worker_port"]

            # send shutdown message to the worker
            message_dict = {
                "message_type": "shutdown",
            }
            utils.send_message(message_dict, worker_host, worker_port)

    def add_job(self, message_dict):
        """Add a job to the job queue."""
        job_id = self.num_jobs_total
        message_dict["job_id"] = job_id
        self.job_queue.append(message_dict)
        self.num_jobs_total += 1

    def run_job(self):
        """Run jobs in the job queue."""
        # keep running jobs until the manager is shutdown
        while not self.signals["shutdown"]:

            # check if there is any job in the queue
            if len(self.job_queue) == 0:
                time.sleep(1)
                continue

            # pop off the first job in the queue
            job = self.job_queue.pop(0)
            ouptut_directory = job["output_directory"]

            # delete the output file if it already exists
            if os.path.exists(ouptut_directory):
                # delete the output file
                shutil.rmtree(ouptut_directory)
            # create the output directory
            os.mkdir(ouptut_directory)
            curr_job_id = job["job_id"]

            job_completed = False

            # Create a shared directory for temporary intermediate files
            prefix = f"mapreduce-shared-job{curr_job_id:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)
                print("DEBUG temp directory", tmpdir)
                temp_output_dir = os.path.join(
                    job["output_directory"], tmpdir)

                while not self.signals["shutdown"] and not job_completed:
                    # check if amy worker is registered
                    if len(self.worker_list) == 0:
                        time.sleep(1)
                        continue
                    # input partition mapping
                    self.input_partition_mapping(job, temp_output_dir)
                    # checkpoint for shutdown
                    if self.signals["shutdown"]:
                        break
                    # reset the number of finished tasks
                    self.num_task_finished = 0
                    # sent reducing task to workers
                    self.reduce(job, temp_output_dir)
                    # checkpoint for shutdown
                    if self.signals["shutdown"]:
                        break
                    # reset the number of finished tasks
                    self.num_task_finished = 0
                    # job completed
                    job_completed = True
                    # clear the mapping task dict
                    self.mapping_tasks = {}
                    # clear the reducing task dict
                    self.reducing_tasks = {}

            LOGGER.info("Cleaned up tmpdir %s", tmpdir)

        print("run_job shutting down")

    def input_partition_mapping(self, job, temp_output_dir):
        """Partition input files and send to mappers."""
        # Scans the input directory
        input_dir = job["input_directory"]
        # Get list of filenames in input directory
        filenames = os.listdir(input_dir)
        # Sort filenames by name
        # mapfiles = sorted(filenames)
        # message_type = "new_map_task"
        executable = job["mapper_executable"]
        num_partitions = job["num_reducers"]

        # Divide the input files into several partitions
        # create a dictionary of task_id to list of input files,
        # and initialize the job status
        for i in range(job["num_mappers"]):
            self.mapping_tasks[i] = []

        for i, file in enumerate(sorted(filenames)):
            # Assign each partition a task_id, starting from 0
            task_id = i % job["num_mappers"]
            file_path = os.path.join(input_dir, file)
            self.mapping_tasks[task_id].append(file_path)

        LOGGER.info("partitioning finished")

        # Send a new_map_task message to a worker
        for map_task in self.mapping_tasks.items():

            map_task = {
                "task_id": map_task[0],
                "input_paths": map_task[1]
            }

            self.sent_task_pipeline("new_map_task", executable,
                                    num_partitions, temp_output_dir, map_task)

            if self.signals["shutdown"]:
                return

        # wait for all mappers to finish
        mapping_completed = self.num_task_finished == job["num_mappers"]
        while not self.signals["shutdown"] and not mapping_completed:
            for i, worker in enumerate(self.worker_list):
                # if the worker is dead and not finishing the task
                if worker["task_id"] in self.mapping_tasks \
                        and worker["state"] in ["dead", "revive"]:
                    task_id = worker["task_id"]
                    self.worker_list[i]["task_id"] = None

                    reassigned_task = {
                        "task_id": task_id,
                        "input_paths": self.mapping_tasks[task_id]
                    }

                    # reassign the mapping task to another worker
                    LOGGER.info("reassigning task %d", task_id)

                    if worker["state"] == "revive":
                        LOGGER.info("reviving (now ready) worker %d", i)
                        self.worker_list[i]["state"] = "ready"

                    self.sent_task_pipeline(
                        "new_map_task", executable, num_partitions,
                        temp_output_dir, reassigned_task)

                    if self.signals["shutdown"]:
                        return

            mapping_completed = self.num_task_finished == job["num_mappers"]
            time.sleep(1)

    def reduce(self, job, temp_output_dir):
        """Send reducing task to workers."""
        # create a dictionary of task_id to list of input file

        # get the list of map files
        executable = job["reducer_executable"]
        num_partitions = None
        # message_type = "new_reduce_task"
        output_directory = job["output_directory"]

        for i in range(job["num_reducers"]):
            # get the list of map files using pathlib.glob,
            # the decisive pattern is last 5 digits
            partition_files = sorted(Path(temp_output_dir).glob(f"*{i:05d}"))
            self.reducing_tasks[i] = [str(file) for file in partition_files]
            LOGGER.info("reducing_tasks[%d] = %s", i, self.reducing_tasks[i])

        # send a new_reduce_task message to a worker
        for task in self.reducing_tasks.items():

            reduce_task = {"task_id": task[0], "input_paths": task[1]}

            self.sent_task_pipeline("new_reduce_task", executable,
                                    num_partitions, output_directory,
                                    reduce_task)

            if self.signals["shutdown"]:
                return

        # wait for all reducers to finish
        reducing_completed = self.num_task_finished == job["num_reducers"]
        while not self.signals["shutdown"] and not reducing_completed:
            for i, worker in enumerate(self.worker_list):
                # if the worker is 'dead' or 'revive'
                # and not finishing the task
                if worker["task_id"] in self.reducing_tasks \
                        and worker["state"] in ["dead", "revive"]:
                    task_id = worker["task_id"]
                    self.worker_list[i]["task_id"] = None

                    if worker["state"] == "revive":
                        LOGGER.info("reviving (now ready) worker %d", i)
                        self.worker_list[i]["state"] = "ready"

                    # find the task item in the reducing_tasks dict
                    reassigned_task = {"task_id": task_id,
                                       "input_paths":
                                       self.reducing_tasks[task_id]
                                       }

                    # reassign the reducing task to a new worker
                    self.sent_task_pipeline(
                        "new_reduce_task", executable, num_partitions,
                        output_directory, reassigned_task)

                    if self.signals["shutdown"]:
                        return

            reducing_completed = self.num_task_finished == job["num_reducers"]
            time.sleep(1)

    def sent_task_pipeline(self, message_type, executable, num_partitions,
                           output_directory, task):
        """Send mapping/reduce task to workers, including error handling."""
        send_task_success, worker_id = self.send_task(
            message_type, executable, num_partitions,
            output_directory, task)

        if self.signals["shutdown"]:
            return

        while not send_task_success and not self.signals["shutdown"]:
            # if the worker is dead and not finishing the task
            LOGGER.info("worker %s dead due to connection error", worker_id)
            self.worker_list[worker_id]["state"] = "dead"
            self.worker_list[worker_id]["task_id"] = None
            # reassign the task to another worker
            send_task_success, worker_id = self.send_task(
                message_type, executable, num_partitions,
                output_directory, task)

    def send_task(self, message_type, executable, num_partitions,
                  output_directory, task):
        """Send mapping task to workers."""
        task_id = task["task_id"]
        input_paths = task["input_paths"]
        # Find a worker that is ready, if not, wait unitl one is ready
        worker_id, worker = self.check_available_worker()
        LOGGER.info("ready worker: worker_id = %s", worker_id)
        # if it is already shutdown, return
        if self.signals["shutdown"]:
            return False, None
        # Send a new_map_task message to the worker
        message_dict = {
            "message_type": message_type,
            "task_id": task_id,
            "input_paths": input_paths,
            "executable": executable,
            "output_directory": output_directory,
            "num_partitions": num_partitions,
            "worker_host": worker["worker_host"],
            "worker_port": worker["worker_port"]
        }

        if message_type == "new_reduce_task":
            # remove the num_partitions key
            message_dict.pop("num_partitions")

        # Send the message to the worker
        send_task_success = utils.send_message(
            message_dict, worker["worker_host"],
            worker["worker_port"])

        # Update the worker state to busy
        self.worker_list[worker_id]["state"] = "busy"
        # add the task_id key to the worker
        self.worker_list[worker_id]["task_id"] = task_id

        return send_task_success, worker_id

    def check_available_worker(self):
        """Check if there is any available worker."""
        # Find a worker that is ready, if not, wait unitl one is ready
        # if shutdown signal is received, return None
        while not self.signals["shutdown"]:
            # check if there is any worker registered
            if len(self.worker_list) == 0:
                time.sleep(1)
                continue
            # allocate task to workers in the order of registration
            for i, worker in enumerate(self.worker_list):
                if worker["state"] == "ready":
                    return i, worker
            # If no worker is found, wait for 1 second and try again
            time.sleep(1)
        return None, None


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir

    handler = utils.main_duplicate(logfile)
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
