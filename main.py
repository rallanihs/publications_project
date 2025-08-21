# ===================== Imports =====================
import os
import time
import shutil
import tempfile
from urllib.parse import urlparse, urljoin
import pdfplumber
import asyncio, random

from google.cloud import storage
from google.cloud import firestore

import pandas as pd
import numpy as np
import base64
import aiohttp
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import WebDriverException
import undetected_chromedriver as uc

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, Any
from functions_framework import http

# ===================== Utilities =====================
def safe_filename(title: str) -> str:
    return "".join(c for c in title if c.isalnum() or c in (' ', '_')).rstrip()

def wiley_doi_to_epdf(url: str) -> str:
    parsed = urlparse(url)
    if 'doi.org' in parsed.netloc:
        doi_suffix = parsed.path.lstrip('/')
        return f"https://onlinelibrary.wiley.com/doi/epdf/{doi_suffix}"
    elif 'onlinelibrary.wiley.com' in parsed.netloc:
        if '/doi/epdf/' in parsed.path or '/doi/pdfdirect/' in parsed.path:
            return url
        doi_suffix = parsed.path.split('/doi/')[-1]
        return f"https://onlinelibrary.wiley.com/doi/epdf/{doi_suffix}"
    else:
        return url

def sage_url_to_epub(url: str) -> str:
    parsed = urlparse(url)
    if "doi.org" in parsed.netloc:
        doi_suffix = parsed.path.lstrip("/")
        return f"https://journals.sagepub.com/doi/epub/{doi_suffix}"
    if "journals.sagepub.com" in parsed.netloc:
        path = parsed.path
        if "/doi/epub/" in path:
            return url
        if "/doi/pdf/" in path:
            doi_suffix = path.split("/doi/pdf/")[-1]
            return f"https://journals.sagepub.com/doi/epub/{doi_suffix}"
        if "/doi/" in path:
            doi_suffix = path.split("/doi/")[-1]
            return f"https://journals.sagepub.com/doi/epub/{doi_suffix}"
    return url

async def try_download(func, url, filepath, max_retries=5, base_delay=2):
    for attempt in range(max_retries):
        try:
            result = await func(url, filepath)
            return result[0] is not None, result[1]
            break
        except Exception as e:
            message = str(e).lower()

            retryable = (
                "429" in message
                or "timeout" in message
                or "502" in message
                or "503" in message
            )

            # Handle 403s with 1 retry only
            if "403" in message and attempt < 1:
                await asyncio.sleep(1 + random.random())
                continue

            if retryable and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                await asyncio.sleep(delay)
                continue
            # Non-retryable, or retries exhausted
            print(f"❌ Download failed for {url}: {e}")
            return False, None
    return False, None

def upload_to_gcs(bucket_name: str, filepath: str) -> dict:
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    filename = filepath.split("/")[-1]
    blob = bucket.blob(filename)

    blob.upload_from_filename(filepath)
    public_url = blob.public_url

    print(f"✅ Uploaded {filepath} as {filename} to bucket {bucket_name}")

    return {
        "filename": filename,
        "public_url": public_url
    }

def extract_text_to_file(pdf_path: str, output_dir: str) -> str:
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    txt_path = os.path.join(output_dir, base_name + ".txt")

    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(text)

    print(f"📝 Extracted text from {pdf_path} -> {txt_path}")
    return txt_path


# ===================== Async Download Functions =====================
async def download_wiley_selenium_async(pdfdirect_url: str, filepath: str):
    """
    Fully async-safe Wiley PDF download using Selenium.
    Preserves epdf conversion and multiple button logic.
    """

    # Wrap the blocking Selenium code in asyncio.to_thread
    def _download():
        save_folder = os.path.dirname(filepath)
        os.makedirs(save_folder, exist_ok=True)

        # Convert pdfdirect → epdf
        epdf_url = pdfdirect_url.replace("/pdfdirect/", "/epdf/")
        print(f"Navigating to: {epdf_url}")

        # Chrome options
        options = uc.ChromeOptions()
        prefs = {
            "download.default_directory": os.path.abspath(save_folder),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--headless")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--start-maximized")

        driver = uc.Chrome(options=options)

        try:
            driver.get(epdf_url)
            time.sleep(3)  # Let page load
            driver.execute_script("window.scrollBy(0, 200);")
            time.sleep(1)

            download_button = None

            # Try multiple known button selectors
            try:
                download_button = driver.find_element(
                    "css selector", 'a.navbar-download[href*="pdfdirect"]'
                )
                print("✅ Found PDFDIRECT button")
            except:
                pass

            if not download_button:
                try:
                    download_button = driver.find_element(
                        "css selector", 'a.coolBar__ctrl.pdf-download'
                    )
                    print("✅ Found COOLBAR PDF button")
                except:
                    pass

            if not download_button:
                try:
                    download_button = driver.find_element(
                        "css selector", 'a.navbar-download[href*="?download=true"]'
                    )
                    print("✅ Found GENERIC NAVBAR PDF button")
                except:
                    pass

            if not download_button:
                print("❌ No known Wiley PDF button found.")
                return None, None

            # Attempt click
            try:
                download_button.click()
                time.sleep(5)  # wait for download to start
            except ElementNotInteractableException:
                print("❌ PDF button found but not interactable — likely paywall or no access.")
                return None, None

            # Wait for PDF to appear
            time.sleep(8)
            pdf_files = [os.path.join(save_folder, f) for f in os.listdir(save_folder) if f.endswith(".pdf")]
            if not pdf_files:
                raise Exception("❌ No PDF found in folder after download.")

            newest_file = max(pdf_files, key=os.path.getctime)
            os.rename(newest_file, filepath)
            print(f"✅ Downloaded & renamed to: {filepath}")
            return filepath, "onlinelibrary.wiley.com"

        finally:
            driver.quit()

    return await asyncio.to_thread(_download)

async def elsevier_selenium_download_async(doi_url: str, filepath: str):
    """
    Async wrapper around Elsevier Selenium download.
    Preserves all original WebDriverWait logic.
    """

    def _download():
        chrome_options = Options()
        chrome_options.headless = True
        chrome_options.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images": 2  # disable images
        })

        driver = uc.Chrome(options=chrome_options)
        wait = WebDriverWait(driver, 15)

        try:
            driver.get(doi_url)
            # Accept cookies if the banner exists
            try:
                consent_button = wait.until(EC.element_to_be_clickable((By.ID, 'onetrust-accept-btn-handler')))
                consent_button.click()
                print("✅ Accepted cookies.")
            except:
                print("⚠️ No cookie banner found or already dismissed.")

            # Find PDF download link
            pdf_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[aria-label^="View PDF"]')))
            pdf_href = pdf_button.get_attribute("href")

            if not pdf_href:
                print("❌ Could not find PDF href.")
                return None, None

            pdf_url = urljoin(driver.current_url, pdf_href)
            print(f"📄 PDF URL found: {pdf_url}")

            # Load the PDF in the browser tab
            driver.get(pdf_url)
            time.sleep(3)

            # Use Chrome DevTools Protocol to print page to PDF
            pdf_data = driver.execute_cdp_cmd("Page.printToPDF", {"printBackground": True})

            # Write the base64-encoded PDF to a file
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "wb") as f:
                f.write(base64.b64decode(pdf_data['data']))
            print(f"✅ PDF saved via Chrome CDP: {filepath}")

            domain = urlparse(pdf_url).netloc
            return filepath, domain
        except Exception as e:
            print(f"❌ Selenium failed for elsevier: {e}")
            driver.quit()
            return None, None

        finally:
            driver.quit()

    return await asyncio.to_thread(_download)

async def download_pmc_playwright(url: str, filepath: str):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            print(f"Navigating to PMC article: {url}")
            await page.goto(url)

            # Find PDF link href
            pdf_link_locator = page.locator("a[href$='.pdf']")
            if await pdf_link_locator.count() == 0:
                print("❌ No PDF link found on page.")
                await browser.close()
                return None, None

            pdf_href = await pdf_link_locator.first.get_attribute("href")
            pdf_url = urljoin(page.url, pdf_href)
            print(f"Found PDF URL: {pdf_url}")

            print(f"Navigating directly to PDF URL and waiting for download to start...")
            async with page.expect_download() as download_info:
                await page.goto(pdf_url)
            download = await download_info.value

            await download.save_as(filepath)
            print(f"✅ Saved PDF to {filepath}")

            await browser.close()
            return filepath, urlparse(pdf_url).netloc

    except Exception as e:
        print(f"❌ Exception: {e}")
        return None, None

async def download_sage_playwright(url: str, filepath: str):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            print(f"Navigating to {url}")
            await page.goto(url)

            # Wait for the download button to appear
            await page.wait_for_selector('a.btn--light.format-download-btn.download')

            print("Clicking download button...")
            try:
                async with page.expect_download(timeout=10000) as download_info:
                    await page.click('a.btn--light.format-download-btn.download')
                download = await download_info.value
            except PlaywrightTimeoutError:
                print("❌ Download did not start: likely requires institutional access or no PDF available.")
                await browser.close()
                return None, None

            await download.save_as(filepath)
            print(f"✅ Saved PDF to {filepath}")

            await browser.close()
            return filepath, "journals.sagepub.com"

    except Exception as e:
        print(f"❌ Exception: {e}")
        return None, None

async def springer_download_playwright(url: str, filepath: str):
    try:
        async with async_playwright() as p:
            headers = {'User-Agent': 'python-requests/2.32.4','From': 'rallan@ihs.gmu.edu'}
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            print(f"➡️ Navigating to Springer page: {url}")
            await page.goto(url, wait_until="domcontentloaded")

            final_url = page.url
            print(f"➡️ Landed on: {final_url}")

            # Decide which selector to use
            if "/chapter/" in final_url:
                print("🔍 Detected CHAPTER layout")
                pdf_link = page.locator(
                    "a.c-pdf-download__link[data-book-pdf='true']"
                )
            elif "/article/" in final_url:
                print("🔍 Detected ARTICLE layout")
                pdf_link = page.locator(
                    "a.c-pdf-download__link[data-article-pdf='true']"
                )
            else:
                print("❌ Unknown Springer page type, cannot decide selector")
                await browser.close()
                return None, None

            # Make sure PDF link exists
            if await pdf_link.count() == 0:
                print("❌ PDF link not found for this layout")
                await browser.close()
                return None, None

            pdf_href = await pdf_link.first.get_attribute("href")
            pdf_url = urljoin(final_url, pdf_href)
            print(f"✅ Found PDF URL: {pdf_url}")

            # Async GET first using aiohttp
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(pdf_url) as resp:
                        if resp.status == 200 and 'application/pdf' in resp.headers.get("Content-Type", ""):
                            content = await resp.read()
                            with open(filepath, "wb") as f:
                                f.write(content)
                            print(f"✅ Downloaded directly: {filepath}")
                            await browser.close()
                            return filepath, urlparse(pdf_url).netloc
                        else:
                            print(f"⚠️ Direct GET failed | Status: {resp.status} | Content-Type: {resp.headers.get('Content-Type')}")
            except Exception as e:
                print(f"⚠️ Direct GET exception: {e}")

            # Playwright fallback
            print("➡️ Trying Playwright download fallback...")
            async with page.expect_download() as download_info:
                await page.goto(pdf_url)
            download = await download_info.value
            await download.save_as(filepath)
            print(f"✅ Downloaded with Playwright: {filepath}")

            await browser.close()
            return filepath, urlparse(pdf_url).netloc

    except PlaywrightTimeoutError:
        print("❌ Playwright timeout. Possibly paywalled or link broken.")
    except Exception as e:
        print(f"❌ Playwright exception: {e}")

    return None, None

async def download_arxiv_playwright(url: str, filepath: str):
    try:
        async with async_playwright() as p:
            headers = {'User-Agent': 'python-requests/2.32.4','From': 'rallan@ihs.gmu.edu'}
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            print(f"🌐 Navigating to arXiv page: {url}")
            await page.goto(url)

            # Locate PDF link robustly with JS (avoids strict mode)
            try:
                pdf_href = await page.evaluate("""
                    () => document.querySelectorAll("a.abs-button.download-pdf")[0].getAttribute('href')
                """)
                pdf_url = urljoin(page.url, pdf_href)
                print(f"✅ Found arXiv PDF URL: {pdf_url}")
            except Exception as e:
                print(f"⚠️ Could not extract PDF link. Reason: {e}")
                await browser.close()
                return None, None

            # Async GET first using aiohttp
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(pdf_url) as resp:
                        if resp.status == 200 and 'application/pdf' in resp.headers.get("Content-Type", ""):
                            content = await resp.read()
                            with open(filepath, "wb") as f:
                                f.write(content)
                            print(f"✅ Downloaded directly: {filepath}")
                            await browser.close()
                            return filepath, urlparse(pdf_url).netloc
                        else:
                            print(f"⚠️ Direct GET failed | Status: {resp.status} | Content-Type: {resp.headers.get('Content-Type')}")
            except Exception as e:
                print(f"⚠️ Direct GET exception: {e}")
                
            # Playwright fallback: force browser download
            print("➡️ Trying Playwright browser download...")
            async with page.expect_download() as download_info:
                await page.goto(pdf_url)
            download = await download_info.value
            await download.save_as(filepath)
            print(f"✅ Downloaded with Playwright: {filepath}")

            await browser.close()
            return filepath, urlparse(pdf_url).netloc

    except PlaywrightTimeoutError:
        print(f"⏱️ Playwright timeout on {url}")
    except Exception as e:
        print(f"❌ Playwright exception: {e}")
    return None, None

async def download_oup_playwright(url: str, filepath: str):
    try:
        async with async_playwright() as p:
            headers = {'User-Agent': 'python-requests/2.32.4','From': 'rallan@ihs.gmu.edu'}
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            print(f"➡️ Navigating to OUP URL: {url}")
            await page.goto(url, wait_until="domcontentloaded")

            final_url = page.url
            print(f"➡️ Landed on: {final_url}")

            parsed = urlparse(final_url)
            save_dir = os.path.dirname(filepath)
            os.makedirs(save_dir, exist_ok=True)

            if "watermark.silverchair.com" in parsed.netloc:
                print(f"✅ PDF found on Silverchair watermark domain: {parsed.netloc}")

                # Try direct GET via aiohttp
                try:
                    async with aiohttp.ClientSession(headers=headers) as session:
                        async with session.get(final_url) as resp:
                            if resp.status == 200 and 'application/pdf' in resp.headers.get("Content-Type", ""):
                                content = await resp.read()
                                with open(filepath, "wb") as f:
                                    f.write(content)
                                print(f"✅ Successfully downloaded OUP PDF: {filepath}")
                                await browser.close()
                                return filepath, parsed.netloc
                            else:
                                print(f"⚠️ Direct GET failed | Status: {resp.status} | Content-Type: {resp.headers.get('Content-Type')}")
                except Exception as e:
                    print(f"⚠️ Direct GET exception: {e}")

                # Playwright fallback
                print(f"➡️ Trying Playwright fallback download...")
                async with page.expect_download() as download_info:
                    await page.goto(final_url)
                download = await download_info.value
                await download.save_as(filepath)
                print(f"✅ Playwright fallback worked: {filepath}")
                await browser.close()
                return filepath, parsed.netloc

            elif "academic.oup.com" in parsed.netloc:
                print("❌ Redirected back to OUP abstract page — likely requires institutional access or login.")
                await browser.close()
                return None, None

            else:
                print(f"❌ Unknown domain after navigation: {parsed.netloc}")
                await browser.close()
                return None, None

    except Exception as e:
        print(f"❌ Playwright exception: {e}")

    return None, None

async def download_via_aiohttp(url: str, filepath: str):
    try:
        print(f"trying aiohttp for {url}")
        headers = {'User-Agent': 'python-requests/2.32.4','From': 'rallan@ihs.gmu.edu'}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30) as resp:
                if resp.status == 200 and resp.content_type == 'application/pdf':
                    content = await resp.read()
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    return filepath, urlparse(url).netloc
    except Exception as e:
        print(f"⚠️ aiohttp failed for {url}: {e}")
    return None, None

async def universal_download(url: str, filepath: str):
    save_dir = os.path.dirname(filepath)
    os.makedirs(save_dir, exist_ok=True)
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            print(f"➡️ Playwright navigating to: {url}")
            await page.goto(url, wait_until="domcontentloaded")

            # First attempt: direct download via <a> click
            try:
                async with page.expect_download(timeout=10000) as download_info:
                    await page.set_content(f'<a href="{url}" download id="dl">Download</a>')
                    await page.click('#dl')
                download = await download_info.value
                await download.save_as(filepath)
                print(f"✅ Playwright download success: {filepath}")
                await browser.close()
                return filepath, urlparse(url).netloc
            except PlaywrightTimeoutError:
                print("⏱️ Playwright direct download timeout, trying automatic-download fallback...")

            # Second attempt: automatic download by navigating
            try:
                async with page.expect_download(timeout=15000) as download_info:
                    await page.goto(url)  # triggers automatic download
                download = await download_info.value
                await download.save_as(filepath)
                print(f"✅ Playwright automatic download success: {filepath}")
                await browser.close()
                return filepath, urlparse(url).netloc
            except PlaywrightTimeoutError:
                print("❌ Playwright automatic download failed.")

            await browser.close()
    except Exception as e:
        print(f"❌ Playwright session error: {e}")
        return None, None
    finally:
        await browser.close()
        
    # Selenium fallback
    def selenium_download():
        try:
            temp_dir = tempfile.mkdtemp(prefix="pdf_dl_")
            print(f"➡️ Selenium using temp download dir: {temp_dir}")

            chrome_options = Options()
            chrome_options.headless = True
            prefs = {
                "download.default_directory": temp_dir,
                "download.prompt_for_download": False,
                "plugins.always_open_pdf_externally": True,
            }
            chrome_options.add_experimental_option("prefs", prefs)

            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)

            timeout = 15
            poll_interval = 0.5
            downloaded_file = None

            for _ in range(int(timeout / poll_interval)):
                files = [f for f in os.listdir(temp_dir) if f.lower().endswith(".pdf")]
                if files:
                    # Pick the newest file
                    files_with_times = [(f, os.path.getctime(os.path.join(temp_dir, f))) for f in files]
                    files_with_times.sort(key=lambda x: x[1], reverse=True)
                    downloaded_file = files_with_times[0][0]

                    # Check if download is stable
                    file_path = os.path.join(temp_dir, downloaded_file)
                    initial_size = os.path.getsize(file_path)
                    time.sleep(1)
                    final_size = os.path.getsize(file_path)
                    if initial_size == final_size and final_size > 0:
                        break
                time.sleep(poll_interval)
            else:
                print("⏱️ Selenium download timeout: no stable PDF found.")
                driver.quit()
                shutil.rmtree(temp_dir)
                return None

            final_path = filepath
            shutil.move(os.path.join(temp_dir, downloaded_file), final_path)
            shutil.rmtree(temp_dir)
            driver.quit()
            print(f"✅ Selenium download success: {final_path}")
            return final_path

        except WebDriverException as e:
            print(f"❌ Selenium WebDriver error: {e}")
            return None
        except Exception as e:
            print(f"❌ Selenium download failed: {e}")
            return None

    result = await asyncio.to_thread(selenium_download)
    if result:
        return filepath, urlparse(url).netloc
    else:
        return None, None

# ===================== Main Async PDF Downloader =====================
async def download_pdf_row(row, headers, output_dir, bucket_name):
    DOI = row['DOI']
    title = row['Publication Title']
    oa_url = row['OpenAlex URL']
    ss_url = row['SS URL']
    oa_pub = row['OpenAlex Publisher']
    ss_pub = row['SS Publisher']
    gcs_filename = None
    gcs_public_url = None
    pdf_upload_error = None
    txt_filename = None
    txt_public_url = None
    txt_upload_error = None

    filename = safe_filename(title) + ".pdf"
    filepath = os.path.join(output_dir, filename)
    domain = None
    success = False

    # Try simple aiohttp download first
    if oa_url:
        result = await download_via_aiohttp(oa_url, filepath)
        if result[0]:
            success = True
            domain = result[1]

    if not success and ss_url and ss_url != oa_url:
        result = await download_via_aiohttp(ss_url, filepath)
        if result[0]:
            success = True
            domain = result[1]

    # Publisher-specific async downloads
    if not success:
        if oa_pub == "onlinelibrary.wiley.com":
            url = wiley_doi_to_epdf(oa_url)
            success, domain = await try_download(download_wiley_selenium_async, url, filepath)
        elif oa_pub == "linkinghub.elsevier.com":
            success, domain = await try_download(elsevier_selenium_download_async, oa_url, filepath)
        elif oa_pub == "link.springer.com":
            success, domain = await try_download(springer_download_playwright, oa_url, filepath)
        elif oa_pub == "journals.sagepub.com":
            fixed_url = sage_url_to_epub(oa_url)
            success, domain = await try_download(download_sage_playwright, fixed_url, filepath)
        elif oa_pub == "pmc.ncbi.nlm.nih.gov":
            success, domain = await try_download(download_pmc_playwright, oa_url, filepath)
        elif oa_pub == "arxiv.org":
            success, domain = await try_download(download_arxiv_playwright, oa_url, filepath)
        elif oa_pub == "academic.oup.com":
            success, domain = await try_download(download_oup_playwright, oa_url, filepath)

    if not success:
        if ss_pub == "onlinelibrary.wiley.com":
            url = wiley_doi_to_epdf(ss_url)
            success, domain = await try_download(download_wiley_selenium_async, url, filepath)
        elif ss_pub == "linkinghub.elsevier.com":
            success, domain = await try_download(elsevier_selenium_download_async, ss_url, filepath)
        elif ss_pub == "link.springer.com":
            success, domain = await try_download(springer_download_playwright, ss_url, filepath)
        elif ss_pub == "journals.sagepub.com":
            fixed_url = sage_url_to_epub(ss_url)
            success, domain = await try_download(download_sage_playwright, fixed_url, filepath)
        elif ss_pub == "pmc.ncbi.nlm.nih.gov":
            success, domain = await try_download(download_pmc_playwright, ss_url, filepath)
        elif ss_pub == "arxiv.org":
            success, domain = await try_download(download_arxiv_playwright, ss_url, filepath)
        elif ss_pub == "academic.oup.com":
            success, domain = await try_download(download_oup_playwright, ss_url, filepath)

    # Final fallback
    if not success:
        success, domain = await try_download(universal_download, oa_url or ss_url, filepath)

    if success and bucket_name:
        # Upload PDF
        try:
            upload_info = upload_to_gcs(bucket_name, filepath)
            gcs_filename = upload_info["filename"]
            gcs_public_url = upload_info["public_url"]
            pdf_upload_error = None
        except Exception as e:
            gcs_filename = None
            gcs_public_url = None
            pdf_upload_error = str(e)

        try:
            txt_path = extract_text_to_file(filepath, output_dir)
            txt_extraction_error = None
        except Exception as e:
            txt_path = None
            txt_extraction_error = str(e)

        if txt_path:
            try:
                txt_upload_info = upload_to_gcs(bucket_name, txt_path)
                txt_filename = txt_upload_info["filename"]
                txt_public_url = txt_upload_info["public_url"]
                txt_upload_error = None
            except Exception as e:
                txt_filename = None
                txt_public_url = None
                txt_upload_error = str(e)
        else:
            txt_filename = None
            txt_public_url = None
            txt_upload_error = None

    db = firestore.Client()

    doc_ref = db.collection("papers").document(DOI)
    update_data = {
        "openAccessStatus": "Open" if success else "Closed",
        "pdfPublicLink": gcs_public_url,
        "textPublicLink": txt_public_url,
        "pdfSource": domain
    }
    
    try:
        doc_ref.set(update_data, merge=True)  # merge=True preserves existing fields
        print(f"✅ Firestore updated for DOI {DOI}")
    except Exception as e:
        print(f"❌ Firestore update failed for DOI {DOI}: {e}")
    
    return {
        'DOI': DOI,
        'Publication Title': title,
        'OA_URL': oa_url,
        'OA_Publisher': oa_pub,
        'SS_URL': ss_url,
        'SS_Publisher': ss_pub,
        'PDF Filepath': filepath if success else None,
        'PDF Source': domain,
        'OA Status' : 'Open' if success else 'Closed',
        'PDF Link in GCS' : gcs_public_url,
        'Text Link in GCS' : txt_public_url,
        'Download_Success': success
    }

# ===================== Batch Downloader =====================
async def download_pdf_batch(df: pd.DataFrame, headers: dict, output_dir: str, bucket_name, max_concurrent=2):
    sem = asyncio.Semaphore(max_concurrent)

    async def sem_task(row):
        async with sem:
            return await download_pdf_row(row, headers, output_dir, bucket_name)

    tasks = [sem_task(row) for _, row in df.iterrows()]
    return await asyncio.gather(*tasks)

# ===================== http wrapper =====================
@http
def download_pdfs_http(request):
    """
    Expects JSON payload:
    {
        "data": [ { "DOI": ..., "Publication Title": ..., "OpenAlex URL": ..., ... }, ... ],
        "headers": { ... }  # optional HTTP headers
    }
    """
    try:
        payload: Dict[str, Any] = request.get_json(silent=True)
        if not payload:
            return {"error": "Invalid or missing JSON payload"}, 400

        data = payload.get("data", [])
        headers = payload.get("headers", [])

        if not data:
            return {"error": "No data provided"}, 400

        df = pd.DataFrame(data)
        bucket_name = "open-access-publications" 
        output_dir = "/tmp"
        os.makedirs(output_dir, exist_ok=True)

        # Run async batch downloader
        results = asyncio.run(download_pdf_batch(df, headers, output_dir, bucket_name))

        return {"results": results}

    except Exception as e:
        return {"error": str(e)}, 500
