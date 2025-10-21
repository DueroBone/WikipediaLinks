import bz2
import io
import multiprocessing as mp
import xml.etree.ElementTree as ET
import re
import json
import time
import queue
import io
import bz2
import multiprocessing as mp
import pickle


class BZ2StreamWrapper(io.RawIOBase):
    _inQueue: int = 0

    def __init__(self, filename, encoding="utf-8"):
        self.filename = filename
        self.encoding = encoding
        self.queue = mp.Queue(maxsize=1000)  # buffer
        self.process = mp.Process(target=self._worker, args=(self.queue,))
        self.process.start()
        self._buffer = b""  # work in bytes
        self._eof = False
        self._inQueue = 0

    def _worker(self, queue):
        """Runs in a separate process, reads bz2 file and sends lines."""
        blob_size = 8192
        try:
            with bz2.open(self.filename, "rb") as f:  # open in binary
                for chunk in iter(lambda: f.read(blob_size), b""):
                    queue.put(chunk)
                    self._inQueue += 1
        finally:
            queue.put(None)  # signal end of stream

    def read(self, size=-1):
        """Provide file-like read(size)."""
        # Fill buffer until we have enough or EOF
        if self._eof and not self._buffer:
            return b""

        while size < 0 or len(self._buffer) < size:
            chunk = self.queue.get()
            self._inQueue -= 1
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


def loadXml(inputFilePath: str, outputFilePath: str):
    """
    Loads and parses the XML file, putting (name, text) tuples into the outputQueue.
    """
    try:
        with BZ2StreamWrapper(inputFilePath) as f:
            with open(outputFilePath, "w+", encoding="utf-8") as outputFile:
                # File loading occurs in background thread
                context = ET.iterparse(f, events=("start", "end"))

                # This thread will only parse XML and enqueue batches
                _, root = next(context)  # get root element
                current_page: list[str] = ["", ""]  # title, text
                for event, elem in context:
                    if event == "start":  # end
                        # print(elem.tag)
                        if elem.tag[-5:] == "title":
                            current_page[0] = elem.text
                        elif elem.tag[-4:] == "text":
                            current_page[1] = elem.text
                            if (
                                current_page[0]
                                and current_page[1]
                                and current_page[1][:9] != "#REDIRECT"
                            ):
                                l = scanLinks(current_page)  # TODO
                                # json.dump(l, outputFile, ensure_ascii=False)
                                print(pickle.dumps(l), file=outputFile)
                                # outputFile.write("\n")
                                # outputFile.flush()
                                current_page = ["", ""]

                            elif current_page[0] or current_page[1]:
                                # print(f"Skipping incomplete page: {current_page}")
                                current_page = ["", ""]
                            else:
                                pass  # empty page, skip
                    elem.clear()
                root.clear()  # free memory
    except Exception as e:
        print(f"Error occurred in LoadXml: {e}")


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


def scanLinks(inputTuple: list[str]) -> tuple[str, list[str]]:
    title, text = inputTuple
    linksRaw = re.findall(r"\[\[.*?\]\]", text or "")
    links = set()
    for link in linksRaw:
        cleaned_link = clean_wikilink(link)
        if cleaned_link:
            links.add(cleaned_link)
    return (title, list(links))


if __name__ == "__main__":
    start_time = time.time()
    loadXml("wikipedia.xml.bz2", "Data/enwiki_links.pkl")
    end_time = time.time()
    print(f"Completed in {end_time - start_time:.2f} seconds.")
