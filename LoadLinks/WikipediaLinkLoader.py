import xml.etree.ElementTree as ET
import re
import pickle
import time
from cProfile import Profile
from pstats import SortKey, Stats

# TODO: Flag redirects
# TODO: MULTI-THREADING
# TODO: Potentially optimize functions


class Site:
    def __init__(self, name: str, links: list[str]):
        self.name = name
        """The name of the page."""

        self.links: set[str] = set()
        """A set to store links to other sites' names."""


def searchAllWikipedia(file_path):
    searchedSites: list[Site] = []

    lastFoundTitle = ""

    # Parse the XML file
    for event, elem in ET.iterparse(file_path):
        try:
            if elem.tag[-5:] == "title":
                lastFoundTitle = elem.text or ""
            if elem.tag[-4:] == "text":  # Adjust tag as necessary
                # print(f"Processing element: \"{elem.tag}\" with text length {len(elem.text)}")
                # Extract the site name and URL from the text
                links = extractLinksFromText(elem)

                site = Site(lastFoundTitle, links)
                if site:
                    # Check if the site already exists in the list
                    foundSite = findSiteInList(searchedSites, site.name)
                    if foundSite is None:
                        searchedSites.append(site)
                    else:
                        print(
                            f"Site {site.name} already exists, pause the debugger here."
                        )
                # Clear the element to free memory
                elem.clear()

        except Exception as e:
            # print(f"Error processing element: \"{elem.tag}\", Error: {e}")
            continue

    return searchedSites


def findSiteInList(sites: list[Site], name: str) -> Site | None:
    """
    Find a site in the list by name.
    Returns the site if found, otherwise None.
    """
    for site in sites:
        if site.name == name:
            return site
    return None


def addSiteToList(sites: list[Site], site: Site):
    """
    Add a site to the list if it does not already exist.
    """
    foundSite = findSiteInList(sites, "site")
    if foundSite is None:
        sites.append(site)


def extractLinksFromText(element: ET.Element) -> list[str]:
    """
    Extract the site name and URL from the text.
    Returns a Site object if successful, otherwise None.
    """
    links = re.findall(r"\[\[.*?\]\]", element.text or "")
    return links


if __name__ == "__main__":
    with Profile() as profile:
        startTime = time.time()
        results = []
        try:
            results = searchAllWikipedia("wikipedia.xml")
        except KeyboardInterrupt:
            pass
        print(f"Execution time: {time.time() - startTime} seconds")
        pickle.dump(results, open("wikipediaLinks.pkl", "wb"))

        (Stats(profile)
        .strip_dirs()
        .sort_stats(SortKey.CUMULATIVE)
        .print_stats()
        )
    while True:
        continue
