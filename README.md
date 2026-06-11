# 🔑 IST-KVS

A project developed for the **Operating Systems** course.

## 📖 Overview

IST-KVS is a concurrent key-value storage system that supports storing, reading, updating and deleting key-value pairs through a hash table.

The project evolved from a batch-processing application into a client-server architecture, supporting concurrent clients, subscription-based notifications, inter-process communication through named pipes and signal handling.

## ✨ Features

* 🔑 Key-value storage using a hash table
* 📂 Batch processing of `.job` files
* 💾 Non-blocking backups using child processes
* 🧵 Multi-threaded job processing
* 🖧 Client-server architecture
* 📨 Named pipe (FIFO) communication
* 🔔 Key subscription and notification system
* 📡 Concurrent client sessions
* ⚠️ Signal handling with `SIGUSR1`

## 🛠️ Built With

* C
* POSIX Threads (pthreads)
* POSIX File System API
* Named Pipes (FIFOs)
* UNIX Signals

## ⚙️ Compilation

Compile the project with:

```bash
make
```

## ▶️ Running

### Server

```bash
./kvs <jobs_directory> <max_threads> <max_backups> <register_fifo>
```

### Client

```bash
./client <client_id> <register_fifo>
```

## 📄 Additional Information

For a detailed description of the project requirements, please refer to the project specification PDFs included in this repository.
