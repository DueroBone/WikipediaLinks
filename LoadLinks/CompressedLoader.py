class Site:
    name: str
    """The name of the page."""
    links: list[str]
    """A list to store links to other sites' names."""

    def __init__(self, name: str, links: list[str]):
        self.name = name

        self.links: list[str] = links


import bz2
import io
import multiprocessing as mp
import xml.etree.ElementTree as ET
import re
import json
import time
from cProfile import Profile
from pstats import SortKey, Stats
import sys
import queue
import os
import WikipediaLinkLoaderMT


import io
import bz2
import multiprocessing as mp


class BZ2StreamWrapper(io.RawIOBase):
    def __init__(self, filename, encoding="utf-8"):
        self.filename = filename
        self.encoding = encoding
        self.queue = mp.Queue(maxsize=100)  # buffer
        self.process = mp.Process(target=self._worker, args=(self.queue,))
        self.process.start()
        self._buffer = b""  # work in bytes

    def _worker(self, queue):
        """Runs in a separate process, reads bz2 file and sends lines."""
        try:
            with bz2.open(self.filename, "rb") as f:  # open in binary
                for chunk in iter(lambda: f.read(8192), b""):
                    queue.put(chunk)
        finally:
            queue.put(None)  # signal end of stream

    def read(self, size=-1):
        """Provide file-like read(size)."""
        # Fill buffer until we have enough or EOF
        while size < 0 or len(self._buffer) < size:
            chunk = self.queue.get()
            if chunk is None:
                break
            self._buffer += chunk

        if size < 0:  # read all
            data, self._buffer = self._buffer, b""
        else:
            data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data

    def readable(self):
        return True

    def close(self):
        super().close()
        self.process.join()


def loadXml(filePath: str, outputQueue: queue.Queue, batchSize: int, numWorkers: int):
    """
    Loads and parses the XML file, putting (name, text) tuples into the outputQueue.
    """
    with Profile() as profile:
        batch = []
        try:
            with BZ2StreamWrapper(filePath) as f:
                context = ET.iterparse(f, events=("start", "end"))
                _, root = next(context)  # get root element
                current_page = {}
                for event, elem in context:
                    if event == "end":
                        if elem.tag[-5:] == "title":
                            current_page["title"] = elem.text
                        elif elem.tag[-4:] == "text":
                            current_page["text"] = elem.text
                            page_data_tuple = (
                                current_page["title"],
                                current_page["text"],
                            )
                            # print(page_data_tuple)
                            if current_page["title"] and current_page["text"]:
                                batch.append(page_data_tuple)
                                current_page = {}

                                if len(batch) >= batchSize:
                                    outputQueue.put(batch)
                                    batch = []
                            else:
                                print(f"Skipping incomplete page: {current_page}")
                root.clear()  # free memory
            outputQueue.put(batch)
            outputQueue.put("Done")  # signal completion to workers
        except Exception as e:
            pass
        Stats(profile, stream=open("Logs/loadXml.txt", "a")).strip_dirs().sort_stats(
            SortKey.CUMULATIVE
        ).print_stats()


def linkScanWorker(inputQueue: queue.Queue, result_queue: queue.Queue):
    with Profile() as profile:
        try:
            while True:
                batch_in = inputQueue.get()
                if batch_in == "Done":  # Poison pill → exit
                    result_queue.put("Done")  # signal completion
                    print(f"[Worker] Shutting down.")
                    break

                batch_out = []
                for title, text in batch_in:
                    links = extractLinksFromText(text)
                    site = Site(title, links)
                    batch_out.append(site)
                result_queue.put(batch_out)
        except Exception as e:
            pass
        Stats(
            profile, stream=open("Logs/linkScanner.txt", "a")
        ).strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()


def extractLinksFromText(text: str) -> list[str]:
    """
    Extract the site name and URL from the text.
    Returns a Site object if successful, otherwise None.
    """
    links = re.findall(r"\[\[.*?\]\]", text or "")
    return links


if __name__ == "__main__":
    numThreads = 1
    batchSize = 1000
    maxQueSize = batchSize * numThreads

    # with Profile() as profile:
    startTime = time.time()
    manager = mp.Manager()
    rawXmlQueue: queue.Queue = manager.Queue(maxsize=maxQueSize)
    batchedQueue: queue.Queue = manager.Queue()
    resultsQueue: queue.Queue = manager.Queue()
    outputFilePath = "linksOutput.jsonl"
    try:

        loaderProcess = mp.Process(
            target=loadXml,
            args=("wikipedia.xml.bz2", rawXmlQueue, batchSize, numThreads),
            name="LoaderProcess",
        )
        loaderProcess.start()

        workerProcess = mp.Process(
            target=linkScanWorker,
            args=(rawXmlQueue, resultsQueue),
            name="WorkerProcess",
        )
        workerProcess.start()

        """
        batchProducerProcess = mp.Process(
            target=WikipediaLinkLoaderMT.batchProducer,
            args=(rawXmlQueue, batchedQueue, batchSize, numThreads),
            name="BatchProducerProcess",
        )
        batchProducerProcess.start()

        workerProcesses = []
        for i in range(numThreads):
            p = mp.Process(
                target=WikipediaLinkLoaderMT.linkScanWorker,
                args=(i, batchedQueue, resultsQueue),
                name=f"WorkerProcess-{i}",
            )
            workerProcesses.append(p)
            p.start()
        """

        unloaderProcess = mp.Process(
            target=WikipediaLinkLoaderMT.deQueueAll,
            args=(resultsQueue, outputFilePath),
            name="UnloaderProcess",
        )
        unloaderProcess.start()
        while True:
            time.sleep(5)
            # continue
            print(  # Print stats
                f"[Main] Raw XML Queue Size: {rawXmlQueue.qsize(): >3}, Batched Queue Size: {batchedQueue.qsize()}, Result Queue Size: {resultsQueue.qsize()} after {(time.time() - startTime):.2f} seconds.",
                file=open("stats.log", "a"),
                flush=True,
            )
    except KeyboardInterrupt:
        pass
    print(f"Execution time: {time.time() - startTime} seconds")
    # Stats(profile).strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()
