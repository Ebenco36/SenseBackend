import os
import re
import json
import time
import random
import string
from io import StringIO

import pandas as pd
import requests
from tqdm import tqdm
from Bio import Entrez, Medline

from src.Commands.DOIEnricher import DOIEnricher  # Progress bar library

class MedlineClass:
    def __init__(self):
        """
        Initializes the MedlineClass with email and API key for Entrez.
        """
        # Entrez.email = "ebenco94@gmail.com"
        # Entrez.api_key = "d4658719b8b55fb6817d221776bbddece608"
        """
        email/api_key: NCBI Entrez credentials
        oa_required: keep only Open Access (via Unpaywall)
        english_only: keep only English-language articles
        """
        email="ebenco94@gmail.com"
        api_key="d4658719b8b55fb6817d221776bbddece608"
        oa_required=True 
        english_only=True
        
        Entrez.email = email
        if api_key:
            Entrez.api_key = api_key
        self.oa_required = oa_required
        self.english_only = english_only
        self.output_dir = "Data/MedlineData"
        os.makedirs(self.output_dir, exist_ok=True)

    # -------------------- utilities --------------------

    def _diag(self, df, label):
        print(f"[DIAG] {label}: {0 if df is None else len(df)}")

    def _generate_random_email(self, domain="gmail.com"):
        """Generate a random email for Unpaywall (they require an email param)."""
        local = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        return f"{local}@{domain}"

    def _clean_doi_from_aid_list(self, aid_list):
        """
        AID often looks like: ["10.1000/xyz [doi]", "S1234-5678(20)30001-2 [pii]"]
        Return "10.1000/xyz" (possibly multiple separated by '; ')
        """
        if not aid_list:
            return ""
        cleaned = []
        for item in aid_list:
            if not isinstance(item, str):
                continue
            # split by whitespace and take the first token that looks like a DOI
            token = item.split()[0]
            if token.lower().startswith("10."):
                cleaned.append(token)
        return "; ".join(cleaned)

    def _is_systematic_review_row(self, title, abstract, pubtype):
        """
        Keep if:
          - Publication Type contains Systematic Review or Meta-Analysis
          - OR title/abstract contains 'systematic' and 'review'
        """
        pt = str(pubtype or "")
        ti = str(title or "")
        ab = str(abstract or "")

        pt_sr = ("systematic review" in pt.lower())
        pt_ma = ("meta-analysis" in pt.lower())

        # simple TI/AB cues
        ti_sr = ("systematic" in ti.lower() and "review" in ti.lower())
        ab_sr = ("systematic" in ab.lower() and "review" in ab.lower())

        return pt_sr or pt_ma or ti_sr or ab_sr

    def _language_is_english(self, lang_field):
        """PubMed LA can be list; accept if any is 'eng' or 'english'."""
        if lang_field is None:
            return False
        if isinstance(lang_field, list):
            s = " ".join(lang_field).lower()
        else:
            s = str(lang_field).lower()
        return ("eng" in s) or ("english" in s)

    def _fetch_unpaywall(self, doi):
        """
        Query Unpaywall for OA info.
        Returns dict with at least keys: is_oa (bool), best_oa_location (dict or None)
        """
        if not doi:
            return {"is_oa": False}
        email = self._generate_random_email()
        url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"[WARN] Unpaywall error for DOI={doi}: {e}")
            return {"is_oa": False}

    # -------------------- NCBI calls --------------------

    def validate_query(self, query: str) -> bool:
        if not query or not query.strip():
            print("Empty query. Skipping.")
            return False
        if len(query.strip()) < 3:
            print("Query too short. Skipping.")
            return False
        return True

    def search_medline(self, query):
        """
        Return all matching PMIDs for the query (handles pagination).
        """
        all_ids = []
        retmax = 10000
        retstart = 0

        while True:
            try:
                handle = Entrez.esearch(db="pubmed", term=query, retmax=retmax, retstart=retstart)
                record = Entrez.read(handle)
                handle.close()
                ids = record.get("IdList", [])
                all_ids.extend(ids)
                if len(ids) < retmax:
                    break
                retstart += retmax
                time.sleep(0.34)  # keep it gentle
            except Exception as e:
                print(f"[ERROR] esearch failed: {e}")
                break

        print(f"Total IDs retrieved: {len(all_ids)}")
        return all_ids

    def fetch_details(self, id_list, batch_size=500):
        """
        Fetch MEDLINE details for IDs. Returns list of Medline dicts.
        Retries missing IDs in smaller batches if needed.
        """
        records = []
        id_list = list(dict.fromkeys(id_list))  # preserve order, dedupe

        # first pass
        print(f"📥 Fetching details for {len(id_list)} PMIDs ...")
        for start in tqdm(range(0, len(id_list), batch_size), desc="📄 efetch", unit="batch"):
            batch_ids = ",".join(id_list[start:start + batch_size])
            try:
                handle = Entrez.efetch(db="pubmed", id=batch_ids, rettype="medline", retmode="text")
                parsed = list(Medline.parse(StringIO(handle.read())))
                handle.close()
                records.extend(parsed)
                time.sleep(0.34)
            except Exception as e:
                print(f"[WARN] efetch batch {start} error: {e}")

        # check missing PMIDs
        returned_pmids = {r.get("PMID", "") for r in records if r.get("PMID")}
        requested_pmids = set(id_list)
        missing = list(requested_pmids - returned_pmids)
        if missing:
            print(f"[DIAG] efetch missing {len(missing)} IDs. Retrying in small batches...")
            for start in range(0, len(missing), 50):
                chunk = ",".join(missing[start:start + 50])
                try:
                    handle = Entrez.efetch(db="pubmed", id=chunk, rettype="medline", retmode="text")
                    parsed = list(Medline.parse(StringIO(handle.read())))
                    handle.close()
                    records.extend(parsed)
                    time.sleep(0.5)
                except Exception as e:
                    print(f"[WARN] retry efetch chunk error: {e}")

        print(f"Total records fetched: {len(records)}")
        return records

    # -------------------- main fetch --------------------

    def fetch(self, queries):
        """
        Run queries, fetch details, enrich with Unpaywall, filter, and save CSV.
        """
        all_rows = []

        for query in queries:
            if not self.validate_query(query):
                continue
            # print(f"\n=== Running query ===\n{query}\n=====================")
            id_list = self.search_medline(query)
            if not id_list:
                print("No results for query.")
                continue

            records = self.fetch_details(id_list)

            print("📝 Processing / enriching records...")
            for r in tqdm(records, desc="🛠 Processing", unit="rec"):
                doi_clean = self._clean_doi_from_aid_list(r.get("AID", []))

                oa_info = {}
                if self.oa_required and doi_clean:
                    # Use first DOI if multiple
                    first_doi = doi_clean.split(";")[0].strip()
                    oa_info = self._fetch_unpaywall(first_doi)

                row = {
                    "pmid": r.get("PMID", ""),
                    "title": r.get("TI", ""),
                    "abstract": r.get("AB", ""),
                    "authors": "; ".join(r.get("AU", [])) if r.get("AU") else "",
                    "publication_date": r.get("DP", ""),
                    "journal": r.get("JT", ""),
                    "country": r.get("PL", ""),
                    "language_raw": "; ".join(r.get("LA", [])) if r.get("LA") else "",
                    "mesh_terms": "; ".join(r.get("MH", [])) if r.get("MH") else "",
                    "publication_type_raw": "; ".join(r.get("PT", [])) if r.get("PT") else "",
                    "doi": doi_clean,
                    "is_oa": bool(oa_info.get("is_oa", False)),
                    "best_oa_url": (oa_info.get("best_oa_location") or {}).get("url") if oa_info else None,
                }
                all_rows.append(row)

        # Build dataframe
        df = pd.DataFrame(all_rows)
        self._diag(df, "raw collected")

        if df.empty:
            print("No data to save.")
            return

        # Normalize column names a bit
        df.rename(columns={
            "publication_type_raw": "publication_type",
            "language_raw": "language"
        }, inplace=True)

        # Deduplicate by PMID (across multiple queries)
        before = len(df)
        df = df.drop_duplicates(subset=["pmid"])
        print(f"[DIAG] dedup pmid: {before} -> {len(df)}")

        # English-only (optional)
        if self.english_only:
            before = len(df)
            df = df[df["language"].str.contains(r"\beng\b|\benglish\b", case=False, na=False)]
            print(f"[DIAG] english filter: {before} -> {len(df)}")

        # Keep only SRs (Publication Type OR TI/AB cues)
        before = len(df)
        sr_mask = df.apply(lambda x: self._is_systematic_review_row(x.get("title"), x.get("abstract"), x.get("publication_type")), axis=1)
        df = df[sr_mask]
        print(f"[DIAG] SR/MA filter: {before} -> {len(df)}")

        # Open Access-only (optional)
        if self.oa_required:
            before = len(df)
            df = df[df["is_oa"] == True]
            print(f"[DIAG] open access filter: {before} -> {len(df)}")

        # Final tidy
        df.fillna({
            "abstract": "No abstract available",
            "authors": "Unknown",
            "doi": "",
            "publication_type": "",
            "country": "",
        }, inplace=True)

        # Save
        out_path = os.path.join(self.output_dir, "medline_results.csv")
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df)} records to {out_path}")