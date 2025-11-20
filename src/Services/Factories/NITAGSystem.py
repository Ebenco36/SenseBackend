import os
import csv
import requests
from bs4 import BeautifulSoup
import glob

def merge_csv_files(output_dir, merged_filename="all_resources.csv"):
    csv_files = sorted(glob.glob(os.path.join(output_dir, "data/resources_page_*.csv")))
    merged_filepath = os.path.join(output_dir, merged_filename)
    if not csv_files:
        print("No per-page CSV files found in directory.")
        return

    header_written = False
    with open(merged_filepath, "w", newline='', encoding='utf-8') as fout:
        writer = None
        for file in csv_files:
            with open(file, "r", encoding='utf-8') as fin:
                reader = csv.DictReader(fin)
                if not header_written:
                    writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    header_written = True
                for row in reader:
                    writer.writerow(row)
    print(f"Merged {len(csv_files)} files into {merged_filepath}")
    
class NITAGResourceScraper:
    BASE_URL = "https://www.nitag-resource.org/resources?page={}"

    def __init__(self, output_dir="output"):
        self.session = requests.Session()
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_resources_from_page(self, soup):
        records = []
        for resource in soup.select('div.listing-item'):
            node_id = resource.get("data-history-node-id", "")

            # Year and Authors extraction
            meta_items = resource.select("div.meta-data span.meta-data__item.fw-medium")
            year = meta_items[0].get_text(strip=True) if len(meta_items) >= 1 else ''
            authors = ""
            if len(meta_items) >= 2:
                authors = ", ".join([a.strip() for a in meta_items[1].stripped_strings if a.strip()])

            # Title & Link
            title = link = ''
            title_tag = resource.select_one("div.heading.h3.fw-semibold a")
            if title_tag:
                title = title_tag.get_text(strip=True)
                link = title_tag.get("href", "")

            # Tags extraction
            tags = [
                t.get_text(strip=True)
                for t in resource.select("div.listing-item__footer div.etiquette span.etiquette__value")
            ]
            tags_str = ", ".join(tags)

            # Rating extraction
            rating = ""
            footer = resource.select_one("div.listing-item__footer")
            if footer:
                field_label = footer.find("div", class_="field__label", string="Rating:")
                if field_label:
                    value_div = field_label.find_next_sibling("div", class_="field__value")
                    if value_div:
                        rating = value_div.get_text(strip=True)

            records.append({
                "node_id": node_id,
                "year": year,
                "authors": authors,
                "title": title,
                "link": link,
                "tags": tags_str,
                "rating": rating,
            })
        return records

    def save_page_to_csv(self, records, page_number):
        filename = f"data/resources_page_{page_number}.csv"
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["node_id", "year", "authors", "title", "link", "tags", "rating"]
            )
            writer.writeheader()
            for row in records:
                writer.writerow(row)

    def fetch_and_save_all_resources(self, max_pages=None):
        page = 0
        while True:
            filename = f"data/resources_page_{page}.csv"
            filepath = os.path.join(self.output_dir, filename)
            if os.path.exists(filepath):
                print(f"Page {page} already downloaded. Skipping.")
                page += 1
                if max_pages and page >= max_pages:
                    break
                continue

            url = self.BASE_URL.format(page)
            print(f"Fetching: {url}")
            resp = self.session.get(url)
            if resp.status_code != 200:
                print(f"Failed to fetch page {page}, status code: {resp.status_code}")
                break
            soup = BeautifulSoup(resp.content, "html.parser")
            page_records = self.extract_resources_from_page(soup)
            if not page_records:
                print("No more resources found.")
                break
            self.save_page_to_csv(page_records, page)
            print(f"Saved {len(page_records)} records to page {page}.")
            page += 1
            if max_pages and page >= max_pages:
                break

if __name__ == "__main__":
    scraper = NITAGResourceScraper(output_dir="./Data/NITAG")
    scraper.fetch_and_save_all_resources()
    merge_csv_files("./Data/NITAG")