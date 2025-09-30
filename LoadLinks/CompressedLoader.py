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
import tracemalloc
import xml.etree.ElementTree as ET
import re
import json
import time
from cProfile import Profile
from pstats import SortKey, Stats
import queue
import io
import bz2
import multiprocessing as mp
import tracemalloc


class BZ2StreamWrapper(io.RawIOBase):
    def __init__(self, filename, encoding="utf-8"):
        self.filename = filename
        self.encoding = encoding
        self.queue = mp.Queue(maxsize=1000)  # buffer
        self.process = mp.Process(target=self._worker, args=(self.queue,))
        self.process.start()
        self._buffer = b""  # work in bytes
        self._eof = False

    def _worker(self, queue):
        """Runs in a separate process, reads bz2 file and sends lines."""
        blob_size = 8192
        try:
            with bz2.open(self.filename, "rb") as f:  # open in binary
                for chunk in iter(lambda: f.read(blob_size), b""):
                    queue.put(chunk)
        finally:
            queue.put(None)  # signal end of stream

    def read(self, size=-1):
        """Provide file-like read(size)."""
        # Fill buffer until we have enough or EOF
        if self._eof and not self._buffer:
            return b""

        while size < 0 or len(self._buffer) < size:
            chunk = self.queue.get()
            if chunk is None:
                self._eof = True
                break  # EOF
            self._buffer += chunk  # de-chunk and place in buffer

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
        tracemalloc.start()
        batch = []
        try:
            with BZ2StreamWrapper(filePath) as f:
                # File loading occurs in background thread
                context = ET.iterparse(f, events=("start", "end"))

                # This thread will only parse XML and enqueue batches
                _, root = next(context)  # get root element
                current_page = ["", ""]
                for event, elem in context:
                    if event == "end":
                        if elem.tag[-5:] == "title":
                            current_page[0] = str(elem.text)
                        elif elem.tag[-4:] == "text":
                            current_page[1] = str(elem.text)
                            page_data_tuple = (
                                current_page[0],
                                current_page[1],
                            )
                            # print(page_data_tuple)
                            if current_page[0] and current_page[1]:
                                batch.append(page_data_tuple)
                                current_page = ["", ""]

                                if len(batch) >= batchSize:
                                    outputQueue.put(batch)
                                    batch = []
                            else:
                                print(f"Skipping incomplete page: {current_page}")
                    # elem.clear()
                root.clear()  # free memory
            outputQueue.put(batch)
            outputQueue.put("Done")  # signal completion to workers
        except KeyboardInterrupt as e:
            current, peak = tracemalloc.get_traced_memory()
            [print(x) for x in tracemalloc.take_snapshot().statistics("lineno")[:20]]
            print(f"Current: {current / 1024**2:.2f} MB; Peak: {peak / 1024**2:.2f} MB")
            pass
        except Exception as e:
            print(f"Error occurred in LoadXml: {e}")
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
        except KeyboardInterrupt as e:
            pass
        except Exception as e:
            print(f"Error occurred in LinkScanWorker: {e}")
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


def clean_wikilink(link: str) -> str | None:
    """
    Clean a single wikilink and return the page title.
    Returns None if the link should be discarded (files, categories, etc.).
    """
    # Only handle things that look like wikilinks
    if not (link.startswith("[[") and link.endswith("]]")):
        return None

    # Strip outer brackets
    link = link[2:-2].strip()

    # Split at pipe '|' and keep the first part (actual target, not display text)
    link = link.split("|")[0].strip()

    # Exclude unwanted namespaces or junk
    bad_prefixes = ("file:", "image:", "category:", "wikipedia:", "wp:", "template:")
    if any(link.lower().startswith(prefix) for prefix in bad_prefixes):
        return None

    return link if link else None


def deQueueAll(inputQueue: queue.Queue, outputFilePath: str):
    """
    Streams cleaned results to a JSON file as {page_name: [links]} per line (JSONL format).
    """
    with open(outputFilePath, "w", encoding="utf-8") as f:
        while True:
            item = inputQueue.get()
            if item is None:
                break
            for subItem in item:
                # Clean each link before writing
                cleaned_links = []
                for l in subItem.links:
                    clean = clean_wikilink(l)
                    if clean:
                        cleaned_links.append(clean)

                # Deduplicate
                cleaned_links = list(set(cleaned_links))

                # Write to file
                json.dump({subItem.name: cleaned_links}, f, ensure_ascii=False)
                f.write("\n")


# def deQueueAll(inputQueue: queue.Queue, outputFilePath: str):
#     """
#     Streams results to a JSON file as {page_name: [links]} per line (JSONL format).
#     """
#     with open(outputFilePath, "w", encoding="utf-8") as f:
#         while True:
#             item = inputQueue.get()
#             if item is None:
#                 break
#             for subItem in item:
#                 # subItem is a Site object
#                 # print(f"Writing links for page: {subItem.name}, {subItem.links}")
#                 json.dump({subItem.name: subItem.links}, f, ensure_ascii=False)
#                 f.write("\n")


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
            target=deQueueAll,
            args=(resultsQueue, outputFilePath),
            name="UnloaderProcess",
        )
        unloaderProcess.start()
        f = open("test.txt", "w+")
        while True:
            time.sleep(5)
            # continue
            print(  # Print stats
                f"[Main] Raw XML Queue Size: {rawXmlQueue.qsize(): >3}, Batched Queue Size: {batchedQueue.qsize()}, Result Queue Size: {resultsQueue.qsize()} after {(time.time() - startTime):.2f} seconds.",
                file=open("stats.log", "a"),
                flush=True,
            )
            f.read()
    except KeyboardInterrupt:
        pass
    print(f"Execution time: {time.time() - startTime} seconds")
    # Stats(profile).strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()
