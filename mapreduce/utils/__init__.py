"""Utils package.

This package is for code shared by the Manager and the Worker.
"""
import os
import tempfile
import logging
import json
import time
import threading
import socket
import click


def send_message(message, host, port):
    """Send a message to a host and port."""
    # create a socket object
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as new_sock:
        try:
            new_sock.connect((host, port))
        except ConnectionRefusedError:
            return False

        message_str = json.dumps(message)
        new_sock.sendall(message_str.encode("utf-8"))

    return True


def recv_message(clientsocket):
    """Receive a message from a socket."""
    clientsocket.settimeout(1)

    with clientsocket:
        message_chunks = []
        while True:
            try:
                data = clientsocket.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)

    # Decode list-of-byte-strings to UTF8 and parse JSON data
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")

    return message_str


def main_duplicate(logfile):
    """Handle duplicate codes in main."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()

    return handler
