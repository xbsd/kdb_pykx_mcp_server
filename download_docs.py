#!/usr/bin/env python3
import re
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import os

# Base URL for PyKX documentation
BASE_URL = "https://code.kx.com/pykx/2.5/"

# Read links.txt and extract href attributes
with open('links.txt', 'r') as f:
    content = f.read()

# Extract all href attributes
href_pattern = r'href="([^"]+)"'
hrefs = re.findall(href_pattern, content)

# Filter and normalize URLs
urls_to_download = set()
for href in hrefs:
    # Skip fragments, external links, and non-html links
    if href.startswith('#'):
        continue
    if href.startswith('http'):
        # Skip external domains
        if not href.startswith('https://code.kx.com/pykx/2.5/'):
            continue
        urls_to_download.add(href)
    elif href.endswith('.html') or href == 'index.html' or '/' in href:
        # Relative URL
        full_url = urllib.parse.urljoin(BASE_URL, href)
        urls_to_download.add(full_url)

# Create output directory
output_dir = Path('pykx_docs')
output_dir.mkdir(exist_ok=True)

print(f"Found {len(urls_to_download)} unique URLs to download")

def download_file(url):
    """Download a single file"""
    try:
        # Extract relative path from URL
        relative_path = url.replace(BASE_URL, '')

        # Create local file path
        local_path = output_dir / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Download the file
        print(f"Downloading: {url}")
        urllib.request.urlretrieve(url, local_path)
        print(f"✓ Downloaded: {relative_path}")
        return (url, True, None)
    except Exception as e:
        print(f"✗ Failed: {url} - {str(e)}")
        return (url, False, str(e))

# Download files in parallel
max_workers = 10
results = []

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(download_file, url): url for url in urls_to_download}

    for future in as_completed(futures):
        results.append(future.result())

# Print summary
successful = sum(1 for _, success, _ in results if success)
failed = len(results) - successful

print(f"\n{'='*60}")
print(f"Download complete!")
print(f"Successful: {successful}/{len(results)}")
print(f"Failed: {failed}/{len(results)}")
print(f"Files saved to: {output_dir.absolute()}")
print(f"{'='*60}")

if failed > 0:
    print("\nFailed downloads:")
    for url, success, error in results:
        if not success:
            print(f"  - {url}: {error}")
