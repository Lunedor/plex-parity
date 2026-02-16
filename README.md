# Plex Parity

Plex Parity is a Streamlit app for tracking missing TV episodes in your Plex library by comparing local episodes against TMDB metadata.

Repository: https://github.com/Lunedor/plex-parity

## What It Does

- Scans TV shows and detects missing aired episodes
- Separates ongoing shows from ended/canceled archive items
- Shows upcoming episode schedule with nearest-airing highlights
- Builds season links for Debrid Media Manager
- Caches scan results for fast reloads
- Supports manual TMDB fixes per show
- Supports ignore/unignore for noisy episodes or whole shows
- Supports scan scope:
  - entire library
  - only watchlisted TV shows

## Tech Stack

- Python 3.10+
- Streamlit
- PlexAPI
- Requests (TMDB API)

## Project Structure

- `app.py`: launch entrypoint
- `ui.py`: Streamlit UI and interaction flow
- `backend.py`: Plex/TMDB integration, cache, scan logic
- `requirements.txt`: Python dependencies
- `config.json`: runtime config (auto-created on first run)
- `plex_cache.json`: scan/cache data (auto-created during use)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/Lunedor/plex-parity.git
cd plex-parity
```

2. (Recommended) Create and activate a virtual environment:

Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

On first run, the app creates `config.json` if missing.

You can configure values in two ways:
- directly in `config.json`
- from the app in `Settings`

Example `config.json`:

```json
{
  "plex_base_url": "http://127.0.0.1:32400",
  "plex_token": "YOUR_PLEX_TOKEN",
  "tmdb_api_key": "YOUR_TMDB_API_KEY",
  "library_name": "TV Shows",
  "scan_scope": "all_library"
}
```

`scan_scope` values:
- `all_library`: scans all TV shows in your selected Plex library
- `watchlist_only`: scans only watchlisted TV shows (movies excluded automatically)

## Getting API Credentials

### Plex token

Common options:
- Use Plex Web and inspect network requests for `X-Plex-Token`
- Use existing Plex scripts/tools if you already have a token

### TMDB API key

1. Create/login TMDB account: https://www.themoviedb.org/
2. Create an API key in account settings
3. Put key in `tmdb_api_key`

## Running

```bash
streamlit run app.py
```

Then open the local URL shown in terminal (usually `http://localhost:8501`).

## Usage Flow

1. Open `Settings`, fill required config, save.
2. Go to `Dashboard` and start scan.
3. Review:
   - Ongoing missing shows
   - Archived missing shows
   - Upcoming episodes
4. If mismatch exists, use TMDB override in show settings or `Maintenance`.
5. If needed, recover ignored items from `Maintenance`.

## Notes

- Posters are fetched from TMDB and may appear after first successful scan/refresh.
- If you rematch titles in Plex, run a new scan; cache reconciliation handles changes.
- Cached results are shown on app reopen until new scans refresh them.

## Troubleshooting

- `Missing config values`:
  - open `Settings` and complete required fields
- `Failed to connect to Plex`:
  - verify `plex_base_url`, token, and local/network access
- Watchlist scope returns no items:
  - ensure your Plex watchlist contains TV shows
  - ensure watchlisted shows exist in your selected Plex library
- No missing episodes but expected:
  - verify TMDB mapping for the show
  - check ignored items in `Maintenance`

## License

No license file is currently included. Add one before wider distribution if needed.
