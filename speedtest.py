
import pickle

class Site:
    def __init__(self, name: str, links: list[str]):
        self.name = name
        """The name of the page."""

        self.links: set[str] = set()
        """A set to store links to other sites' names."""

links = pickle.load(open("wikipediaLinks.pkl", "rb"))

print(f"Loaded {len(links)} links")
for site in links:
    print(f"Site: {site.name}, Links: {len(site.links)}")
    for link in list(site.links)[:5]:
        print(f"  - {link}")