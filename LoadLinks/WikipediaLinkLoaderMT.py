import xml.etree.ElementTree as ET
import re
import json
import time
from cProfile import Profile
from pstats import SortKey, Stats
import sys
import queue
import os
import multiprocessing as mp

# TODO: Flag redirects


class Site:
    name: str
    links: list[str]

    def __init__(self, name: str, links: list[str]):
        self.name = name
        """The name of the page."""

        self.links: list[str] = links
        """A list to store links to other sites' names."""


def loadXml(file_path: str, outputQueue: queue.Queue, batchSize: int, numThreads: int):
    batch = []
    for event, elem in ET.iterparse(file_path):
        if elem.tag[-5:] == "title" or elem.tag[-4:] == "text":
            batch.append((elem.tag, elem.text or ""))
            elem.clear()
            if len(batch) >= batchSize:
                outputQueue.put(batch)
                batch = []
    if batch:
        outputQueue.put(batch)
    for _ in range(numThreads):
        outputQueue.put(None)  # Signal completion to workers
    print("[Loader] Finished reading XML.")


def batchProducer(
    inputQueue: queue.Queue, outputQueue: queue.Queue, batchSize: int, numThreads: int
):
    # Now inputQueue gives batches of (tag, text) pairs
    batch = []
    lastTitle = ""
    while True:
        item = inputQueue.get()
        if item is None:  # Poison pill → exit
            if batch:
                outputQueue.put(batch)  # send remaining batch
            for _ in range(numThreads):
                outputQueue.put(None)  # signal completion to workers
            print("[BatchProducer] Shutting down.")
            break

        # item is a batch of (tag, text) pairs
        for tag, text in item:
            if tag[-5:] == "title":
                lastTitle = text
            if tag[-4:] == "text":
                out = (lastTitle, text)
                batch.append(out)
            if len(batch) >= batchSize:
                outputQueue.put(batch)
                batch = []


def linkScanWorker(worker_id: int, inputQueue: queue.Queue, result_queue: queue.Queue):
    while True:
        batch = inputQueue.get()
        if batch is None:  # Poison pill → exit
            result_queue.put(None)  # signal completion
            print(f"[Worker-{worker_id}] Shutting down.")
            break

        results = []
        for name, text in batch:
            links = extractLinksFromText(text)
            site = Site(name, links)
            results.append(site)
        result_queue.put(results)


def extractLinksFromText(text: str) -> list[str]:
    """
    Extract the site name and URL from the text.
    Returns a Site object if successful, otherwise None.
    """
    links = re.findall(r"\[\[.*?\]\]", text or "")
    return links


def deQueueAll(inputQueue: queue.Queue, outputFilePath: str):
    """
    Streams results to a JSON file as {page_name: [links]} per line (JSONL format).
    """
    with open(outputFilePath, "w", encoding="utf-8") as f:
        while True:
            item = inputQueue.get()
            if item is None:
                break
            for subItem in item:
                # subItem is a Site object
                # print(f"Writing links for page: {subItem.name}, {subItem.links}")
                json.dump({subItem.name: subItem.links}, f, ensure_ascii=False)
                f.write("\n")


if __name__ == "__main__":
    numThreads = 1
    batchSize = 1000
    maxQueSize = batchSize * numThreads

    with Profile() as profile:
        startTime = time.time()
        manager = mp.Manager()
        rawXmlQueue: queue.Queue = manager.Queue(maxsize=maxQueSize)
        batchedQueue: queue.Queue = manager.Queue()
        resultsQueue: queue.Queue = manager.Queue()
        outputFilePath = "wikipediaLinks.jsonl"
        try:

            loaderProcess = mp.Process(
                target=loadXml,
                args=("wikipedia.xml", rawXmlQueue, batchSize, numThreads),
                name="LoaderProcess",
            )
            loaderProcess.start()

            batchProducerProcess = mp.Process(
                target=batchProducer,
                args=(rawXmlQueue, batchedQueue, batchSize, numThreads),
                name="BatchProducerProcess",
            )
            batchProducerProcess.start()

            workerProcesses = []
            for i in range(numThreads):
                p = mp.Process(
                    target=linkScanWorker,
                    args=(i, batchedQueue, resultsQueue),
                    name=f"WorkerProcess-{i}",
                )
                workerProcesses.append(p)
                p.start()

            unloaderProcess = mp.Process(
                target=deQueueAll,
                args=(resultsQueue, outputFilePath),
                name="UnloaderProcess",
            )
            unloaderProcess.start()
            while True:
                time.sleep(5)
                print(
                    f"[Main] Raw XML Queue Size: {rawXmlQueue.qsize(): >3}, Batched Queue Size: {batchedQueue.qsize()}, Result Queue Size: {resultsQueue.qsize()} after {(time.time() - startTime):.2f} seconds."
                )
        except KeyboardInterrupt:
            pass
        print(f"Execution time: {time.time() - startTime} seconds")
        Stats(profile).strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()
