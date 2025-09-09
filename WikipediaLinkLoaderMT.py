import xml.etree.ElementTree as ET
import re
import pickle
import time
from cProfile import Profile
from pstats import SortKey, Stats
import sys
import queue
import os
import multiprocessing as mp

# TODO: Flag redirects
# TODO: MULTI-THREADING
# TODO: Potentially optimize functions


class Site:
    def __init__(self, name: str, links: list[str]):
        self.name = name
        """The name of the page."""

        self.links: set[str] = set()
        """A set to store links to other sites' names."""


def loadXml(file_path: str, outputQueue: queue.Queue):
    for event, elem in ET.iterparse(file_path):
        if elem.tag[-5:] == "title" or elem.tag[-4:] == "text":
            outputQueue.put((elem.tag, elem.text or ""))
            elem.clear()
    for _ in range(numThreads):
        outputQueue.put(None)  # Signal completion to workers
    print("[Loader] Finished reading XML.")


def batchProducer(
    inputQueue: queue.Queue, outputQueue: queue.Queue, batchSize: int, numThreads: int
):
    # Take in the input from inputQueue and produce batches
    # grouped by pairs of title and text
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

        if item[0][-5:] == "title":
            lastTitle = item[1]
        if item[0][-4:] == "text":
            out = (lastTitle, item[1])
            batch.append(out)
        if len(batch) >= batchSize:
            outputQueue.put(batch)  # send batch to workers
            batch = []  # reset batch


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


def deQueueAll(inputQueue: queue.Queue, outputList):
    while True:
        item = inputQueue.get()
        for subItem in item:
            outputList.append(subItem)


if __name__ == "__main__":

    # numThreads = int((os.cpu_count() or 8) / 2)
    numThreads = 1
    batchSize = 1000
    maxQueSize = batchSize * numThreads


    with Profile() as profile:
        startTime = time.time()
        manager = mp.Manager()
        rawXmlQueue: queue.Queue = manager.Queue(maxsize=maxQueSize)
        batchedQueue: queue.Queue = manager.Queue()
        resultsQueue: queue.Queue = manager.Queue()
        resultsList = manager.list()
        try:
            loaderProcess = mp.Process(
                target=loadXml,
                args=("wikipedia.xml", rawXmlQueue),
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
                target=deQueueAll, args=(resultsQueue, resultsList), name="UnloaderProcess"
            )
            unloaderProcess.start()
            lastResultSize = 0
            lastResultSameCount = 0
            while True:
                time.sleep(1)
                resultSize = len(resultsList)
                if resultSize == lastResultSize:
                    lastResultSameCount += 1
                else:
                    lastResultSameCount = 0
                lastResultSize = resultSize
                if lastResultSameCount >= 30:
                    print("[Main] No new results for 10 seconds, shutting down.")
                    [p.kill() for p in workerProcesses]
                    batchProducerProcess.kill()
                    loaderProcess.kill()
                    unloaderProcess.kill()
                    raise KeyboardInterrupt
                    break
                print(
                    f"[Main] Raw XML Queue Size: {rawXmlQueue.qsize(): >3}, Batched Queue Size: {batchedQueue.qsize()}, Result Queue Size: {resultsQueue.qsize()}, Result list size {len(resultsList): >9} after {(time.time() - startTime):.2f} seconds"
                )
        except KeyboardInterrupt:
            pass
        print(f"Execution time: {time.time() - startTime} seconds")
        newList = [site for site in resultsList]
        pickle.dump(newList, open("wikipediaLinks.pkl", "wb"))

        Stats(profile).strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()
