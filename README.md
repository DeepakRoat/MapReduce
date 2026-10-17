# Distributed MapReduce Engine in Go

A robust, high-performance, fault-tolerant MapReduce execution engine built in Go. Designed with $O(1)$ task scheduling, worker quarantine heuristics for straggler mitigation, and atomic intermediate storage guarantees.

---

**Brief Overview of MapReduce :**
MapReduce is a programming model and processing technique for generating large data sets with a parallel, distributed algorithm on a cluster. It consists of a **Map** step that processes and transforms input data into key/value pairs, and a **Reduce** step that merges all intermediate values associated with the same key.


## Architectural Highlights

- **$O(1)$ Monotonic Task Scheduling:** Eliminates $O(N)$ slice iterations by maintaining a monotonic in-flight queue (`inFlightQueue`) for constant-time straggler timeout detection ($>10\text{s}$).
- **Worker Health & Quarantine Engine:** Tracks worker reliability using a min-heap priority queue. Penalizes failing workers using exponential penalty scoring $S \leftarrow S + (t_{\text{fail}} + 1.0)^{0.9}$ while rewarding successful task completions with exponential score decay ($S \leftarrow S \times 0.9$).
- **Atomic Storage & Crash Resilience:** Eliminates partial-write data corruption during worker crashes by combining temporary file generation (`os.CreateTemp`) with atomic filesystem renames (`os.Rename`).
- **Event-Driven Phase Synchronization:** Coordinates phase transitions ($\text{Map} \rightarrow \text{Reduce} \rightarrow \text{Done}$) deterministically using cursor tracking (`minUnfinishedIdx` and `nextUnassignedIdx`).
- **Inter-Process Communication (IPC):** Uses UNIX domain sockets (HTTP RPC protocol) for low-latency communication between the Coordinator and Worker processes.

---

## Repository Structure

```text
MapReduce/
├── coordinator.go   # Master server, job phase tracking, and task scheduling
├── heap.go          # Min-heap implementation for worker quarantine scoring
├── rpc.go           # RPC payload definitions & UNIX socket configuration
├── types.go         # Core data structures (Task, WorkerInfo, State Enums)
└── worker.go        # Worker runtime (Map/Reduce execution loops)
```

---

## File Overview

| File | Responsibilities |
| :--- | :--- |
| `coordinator.go` | Manages global job state, assigns tasks via `GetTask`, processes completion/failure reports in `ReportTask`, monitors timeouts, and triggers phase transitions. |
| `worker.go` | Polls the Coordinator for tasks, executes Map (FNV-1a partition hashing) and Reduce operations, manages intermediate files, and reports task status. |
| `heap.go` | Implements `container/heap.Interface` for `WorkerMinHeap` to rank and quarantine workers based on failure rates. |
| `rpc.go` | Defines typed RPC contracts (`TaskArgs`, `TaskReply`, `ReportTaskArgs`, `ReportTaskReply`) and socket path resolvers. |
| `types.go` | Houses system-wide data structures (`Task`, `WorkerInfo`) and phase enumerations (`MapPhase`, `ReducePhase`, `DonePhase`). |

---

## System Design

### 1. Task Lifecycle & State Machine
Tasks transition through three distinct states: `TaskUnassigned` $\rightarrow$ `TaskInProgress` $\rightarrow$ `TaskCompleted`.
- When a worker requests work, the Coordinator fetches the next unassigned task index in $O(1)$ time.
- If no unassigned tasks remain, it inspects the head of `inFlightQueue`. If the oldest active task exceeds 10 seconds, it is marked as timed out and reassigned immediately.
- If all active tasks are running within the timeout threshold, the worker is instructed to `WAIT`.

### 2. Intermediate File Pipeline
- **Map Phase:** Reads input splits, executes the user-defined Map function, partitions key-value pairs into `NReduce` buckets using FNV-1a hashing (`ihash(key) % nReduce`), and writes intermediate JSON files (`mr-MapID-ReduceID`).
- **Reduce Phase:** Collects intermediate files corresponding to partition `R` (`mr-*-R`), sorts key-value pairs by key, executes the Reduce function, and atomically produces `mr-out-R`.

### 3. Fault Tolerance & Worker Quarantine
If a worker fails or exceeds the timeout threshold:
1. The task status resets to `TaskUnassigned` for instant reallocation.
2. The failing worker's fault score increases based on elapsed job runtime.
3. The worker is pushed into a quarantine min-heap (`quarantineWorkerLocked`).

---

## Usage Example

### Starting the Coordinator
```go
package main

import (
	"time"
	"your-module/MapReduce"
)

func main() {
	inputFiles := []string{"pg-being_ernest.txt", "pg-dorian_gray.txt"}
	nReduce := 10

	coordinator := MapReduce.MakeCoordinator(inputFiles, nReduce)
	for !coordinator.Done() {
		time.Sleep(time.Second)
	}
}
```

### Starting a Worker
```go
package main

import "your-module/MapReduce"

func main() {
	MapReduce.Worker(mapf, reducef)
}
```