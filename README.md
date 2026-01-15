# 🎧 ABS Ratings Updater

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Unraid%20%7C%20Docker-orange)
![Vibecoding](https://img.shields.io/badge/Vibecoded-✨-purple)

A fully automated script designed for **Audiobookshelf (ABS)** on **Unraid**. It scrapes ratings from **Audible** and **Goodreads** and injects them directly into your audiobook descriptions, keeping your library metadata rich and up-to-date.

## ✨ Features

* **Dockerized Execution:** Runs in a temporary `python:3.11-slim` container. No messy dependency installation on your host system.
* **Dual Source Scraping:** Fetches ratings from Audible (supports multiple regions: `.com`, `.de`, `.co.uk`, etc.) and Goodreads.
* **Smart ASIN Search:** Automatically attempts to find missing ASINs based on Title, Author, and Duration if they are missing in ABS.
* **Visual Ratings:** Adds "Moon" emojis (e.g., 🌕🌕🌗🌑🌑) to your descriptions for a quick visual overview.
* **Recycling:** Preserves existing ratings if the scraper cannot find new data (prevents data loss).
* **Unraid Notifications:** Sends a summary notification to the Unraid WebGUI upon completion.
* **Rate Limit Safe:** Includes batching and sleep timers to prevent API bans.

## 🚀 Installation (Unraid)

This script is optimized for the **User Scripts** plugin on Unraid.

1.  **Prepare the Directory:**
    Create a folder for your scripts (e.g., inside your appdata):
    ```bash
    /mnt/user/appdata/audiobookshelf/abs_scripts/
    ```
2.  **Download Files:**
    Place `abs_ratings.py` and `userscript.sh` into this folder.
3.  **Setup User Script:**
    * Open the **User Scripts** plugin in Unraid.
    * Add a new script (e.g., "ABS Ratings").
    * Paste the contents of `userscript.sh` into the script editor.
4.  **Edit Configuration:**
    Update the variables in the `userscript.sh` file to match your environment (see below).

## ⚙️ Configuration

Open `userscript.sh` and adjust the **CONFIGURATION** section:

| Variable | Description | Example |
| :--- | :--- | :--- |
| `SCRIPT_DIR` | The absolute path to the folder containing `abs_ratings.py`. | `/mnt/user/appdata/...` |
| `ABS_URL` | Your internal Audiobookshelf URL (no trailing slash). | `http://192.168.1.10:13378` |
| `API_TOKEN` | Your ABS API Token (Bearer Token). | `eyJhbG...` |
| `LIBRARY_IDS` | Comma-separated list of Library IDs to scan. | `library-uuid-1,library-uuid-2` |
| `BATCH_SIZE` | How many items to process in one run. | `250` |
| `DRY_RUN` | If `true`, no changes are saved to ABS. Good for testing. | `false` |

> **Note on Paths:** The Python script currently expects the history JSON files to be located at `/mnt/user/appdata/audiobookshelf/abs_scripts/`. If your path is different, please update `HISTORY_FILE` and `FAILED_FILE` inside `abs_ratings.py`.

## 🖼️ What it looks like

The script searches for `⭐ Ratings & Infos` in your description. If not found, it creates it. If found, it updates it.

**Example output in Audiobookshelf:**

> **⭐ Ratings & Infos**<br>
> Audible (124):<br>
> 🏆 🌕🌕🌕🌕🌗 4.6 / 5 - Overall<br>
> 🎙️ 🌕🌕🌕🌕🌕 4.8 / 5 - Performance<br>
> 📖 🌕🌕🌕🌕🌑 4.2 / 5 - Story<br>
> Goodreads (450):<br>
> 🏆 🌕🌕🌕🌕🌗 4.4 / 5 - Rating<br>
> ⭐<br>
> <br>
> *[Original Description follows here...]*

## 🛠️ How it Works

1.  The bash script initiates a `docker run` command.
2.  It mounts your script directory into the container and passes your config as Environment Variables.
3.  The container installs `requests`, `beautifulsoup4`, and `lxml`.
4.  It executes `abs_ratings.py`, which:
    * Scans your libraries.
    * Checks if items need an update (older than 90 days).
    * Scrapes metadata.
    * Updates the ABS description via API.

---
<sub>Vibecoded: Concept & Logic by the developer, refined with AI assistance.</sub>
