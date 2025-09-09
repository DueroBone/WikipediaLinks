import xml.etree.ElementTree as ET
import multiprocessing as mp
import threading
import time


def xml_producer(filename, task_queue, num_workers, tag="record"):
    """
    Parse XML incrementally and feed elements into the task queue.
    """
    context = ET.iterparse(filename, events=("end",))
    for event, elem in context:
        if elem.tag == tag:
            task_queue.put(elem)  # send chunk to workers
            elem.clear()          # free memory

    # Done → send poison pills to workers
    for _ in range(num_workers):
        task_queue.put(None)
    print("[Producer] Finished reading XML.")


def worker(worker_id, task_queue, result_queue):
    """
    Worker process that consumes XML elements and sends results back.
    """
    while True:
        elem = task_queue.get()
        if elem is None:  # Poison pill → exit
            result_queue.put(None)  # signal completion
            print(f"[Worker-{worker_id}] Shutting down.")
            break

        # Example processing: count children
        result = (elem.tag, len(list(elem)))
        result_queue.put(result)

        # Simulate work
        time.sleep(0.1)


if __name__ == "__main__":
    filename = "large.xml"  # replace with your XML file
    num_workers = 4

    # Queues for tasks and results
    task_queue = mp.Queue(maxsize=100)
    result_queue = mp.Queue()

    # Start worker processes
    workers = [
        mp.Process(target=worker, args=(i, task_queue, result_queue))
        for i in range(num_workers)
    ]
    for w in workers:
        w.start()

    # Start producer thread
    producer_thread = threading.Thread(
        target=xml_producer,
        args=(filename, task_queue, num_workers),
        daemon=True,
    )
    producer_thread.start()

    # Collect results in main process
    finished_workers = 0
    total_processed = 0
    child_counts = []

    while finished_workers < num_workers:
        result = result_queue.get()
        if result is None:
            finished_workers += 1
        else:
            tag, count = result
            total_processed += 1
            child_counts.append(count)

    # Wait for producer to finish
    producer_thread.join()
    for w in workers:
        w.join()

    print("✅ All XML chunks processed.")
    print(f"Total processed: {total_processed}")
    print(f"Average children per element: {sum(child_counts) / len(child_counts):.2f}")
