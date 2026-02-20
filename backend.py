import json
import os
import re
import shutil
import subprocess
from datetime import datetime
from difflib import SequenceMatcher
from urllib.parse import urlencode

import requests
from plexapi.server import PlexServer

CONFIG_FILE = "config.json"
CACHE_FILE = "plex_cache.json"
SEASON_CACHE_FILE = "tmdb_season_cache.json"
SCAN_META_FILE = "scan_meta.json"
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w342"
ENDED_STATUSES = {"Ended", "Canceled"}
REQUEST_TIMEOUT = 15
SCAN_BATCH_SIZE = 3
HTTP_SESSION = requests.Session()
ENV_CONFIG_MAP = {
    "PLEX_BASE_URL": "plex_base_url",
    "PLEX_TOKEN": "plex_token",
    "TMDB_API_KEY": "tmdb_api_key",
    "PLEX_LIBRARY_NAME": "library_name",
    "PLEX_SCAN_SCOPE": "scan_scope",
    "INCLUDE_DMM_LINK": "include_dmm_link",
    "INDEXER_PROVIDER": "indexer_provider",
    "INDEXER_API_KEY": "indexer_api_key",
    "INDEXER_WEB_URL_TEMPLATE": "indexer_web_url_template",
    # Backward compatibility env names.
    "USENET_PROVIDER": "indexer_provider",
    "USENET_API_KEY": "indexer_api_key",
    "USENET_WEB_URL_TEMPLATE": "indexer_web_url_template",
    # Backward compatibility with previous env names.
    "MISSING_LINK_PROVIDER": "secondary_missing_link_provider",
    "MISSING_LINK_BASE_URL": "secondary_missing_link_base_url",
    "MISSING_LINK_API_KEY": "secondary_missing_link_api_key",
}

DEFAULT_CONFIG = {
    "plex_base_url": "http://127.0.0.1:32400",
    "plex_token": "",
    "tmdb_api_key": "",
    "library_name": "TV Shows",
    "scan_scope": "all_library",
    "include_dmm_link": True,
    "indexer_provider": "none",
    "indexer_api_key": "",
    "indexer_web_url_template": "",
    "indexer_provider_profiles": {},
    # Backward compatibility keys.
    "usenet_provider": "none",
    "usenet_api_key": "",
    "usenet_web_url_template": "",
    "usenet_provider_profiles": {},
    "dmm_base_url": "https://debridmediamanager.com",
    # Backward compatibility keys (legacy).
    "secondary_missing_link_provider": "none",
    "secondary_missing_link_base_url": "",
    "secondary_missing_link_api_key": "",
}


def normalize_title(value):
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def load_config():
    if not os.path.exists(CONFIG_FILE):
        save_config(DEFAULT_CONFIG)
        return apply_env_overrides(DEFAULT_CONFIG.copy())

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return DEFAULT_CONFIG.copy()
            merged = DEFAULT_CONFIG.copy()
            merged.update(data)
            return apply_env_overrides(merged)
    except (json.JSONDecodeError, OSError):
        return apply_env_overrides(DEFAULT_CONFIG.copy())


def save_config(config):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def apply_env_overrides(config):
    output = dict(config)
    for env_name, config_key in ENV_CONFIG_MAP.items():
        value = os.getenv(env_name)
        if value is not None and value.strip():
            output[config_key] = value.strip()
    output["scan_scope"] = normalize_scan_scope(output.get("scan_scope"))
    output["include_dmm_link"] = normalize_bool_flag(output.get("include_dmm_link"), default=True)
    output["indexer_provider"] = normalize_indexer_provider(
        output.get("indexer_provider", output.get("usenet_provider"))
    )
    # Legacy fallback: if new keys are empty, reuse older secondary_* keys.
    if output.get("indexer_provider") == "none":
        output["indexer_provider"] = normalize_indexer_provider(
            output.get("secondary_missing_link_provider")
        )
    if not str(output.get("indexer_api_key", "") or "").strip():
        output["indexer_api_key"] = str(
            output.get("usenet_api_key", output.get("secondary_missing_link_api_key", "")) or ""
        ).strip()
    if not str(output.get("indexer_web_url_template", "") or "").strip():
        output["indexer_web_url_template"] = str(output.get("usenet_web_url_template", "") or "").strip()
    if not isinstance(output.get("indexer_provider_profiles"), dict):
        legacy_profiles = output.get("usenet_provider_profiles")
        output["indexer_provider_profiles"] = legacy_profiles if isinstance(legacy_profiles, dict) else {}
    return output


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    return {}


def load_tmdb_season_cache():
    if os.path.exists(SEASON_CACHE_FILE):
        try:
            with open(SEASON_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_tmdb_season_cache(cache):
    try:
        with open(SEASON_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError:
        return


def load_results_map_from_cache(cache=None):
    source = cache if isinstance(cache, dict) else load_cache()
    results_map = {}
    for key, entry in source.items():
        if not isinstance(entry, dict):
            continue
        title = entry.get("title")
        if not title:
            continue
        results_map[key] = {
            "cache_key": key,
            "title": title,
            "year": entry.get("year"),
            "status": entry.get("status", "Unknown"),
            "tmdb_id": entry.get("tmdb_id"),
            "imdb_id": entry.get("imdb_id"),
            "missing": entry.get("missing", []) or [],
            "next_air": entry.get("next_air"),
            "upcoming_all": entry.get("upcoming_all", []) or [],
            "tmdb_source": entry.get("tmdb_source", "cache"),
            "poster_url": entry.get("poster_url"),
        }
    return results_map


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def load_scan_meta():
    if os.path.exists(SCAN_META_FILE):
        try:
            with open(SCAN_META_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_scan_meta(meta):
    try:
        with open(SCAN_META_FILE, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
    except OSError:
        return


def mark_full_scan_completed():
    meta = load_scan_meta()
    meta["full_scan_completed"] = True
    meta["full_scan_completed_at"] = datetime.now().isoformat(timespec="seconds")
    save_scan_meta(meta)


def has_full_scan_completed():
    meta = load_scan_meta()
    return bool(meta.get("full_scan_completed"))


def cache_has_show_entries(cache=None):
    source = cache if isinstance(cache, dict) else load_cache()
    for _, entry in source.items():
        if isinstance(entry, dict) and entry.get("title"):
            return True
    return False


def validate_config(config):
    missing = []
    if not config.get("plex_base_url", "").strip():
        missing.append("plex_base_url")
    if not config.get("plex_token", "").strip():
        missing.append("plex_token")
    if not config.get("tmdb_api_key", "").strip():
        missing.append("tmdb_api_key")
    if not config.get("library_name", "").strip():
        missing.append("library_name")
    return missing


def normalize_scan_scope(scope):
    if scope == "watchlist_only":
        return "watchlist_only"
    return "all_library"


def normalize_indexer_provider(provider):
    normalized = str(provider or "").strip().lower()
    aliases = {"tornzab": "torznab"}
    normalized = aliases.get(normalized, normalized)
    if normalized in {"newznab", "nzbhydra", "torznab", "prowlarr", "torbox", "jackett", "custom"}:
        return normalized
    return "none"


def normalize_missing_link_provider(provider):
    # Backward compatible alias.
    return normalize_indexer_provider(provider)


def normalize_bool_flag(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def show_cache_key(show):
    return str(show.ratingKey)


def extract_guid_set(item):
    guids = set()
    for guid in getattr(item, "guids", []) or []:
        raw = getattr(guid, "id", "") or str(guid)
        if raw:
            guids.add(raw.lower())
    raw_guid = getattr(item, "guid", None)
    if raw_guid:
        guids.add(str(raw_guid).lower())
    return guids


def filter_shows_for_scan(plex, shows, scan_scope):
    scope = normalize_scan_scope(scan_scope)
    if scope != "watchlist_only":
        return shows, None

    try:
        account = plex.myPlexAccount()
        watchlist_shows = account.watchlist(libtype="show")
    except Exception as exc:
        return [], f"Failed to load Plex watchlist: {exc}"

    if not watchlist_shows:
        return [], "Watchlist mode is enabled but no TV show is in watchlist."

    watchlist_guids = set()
    watchlist_title_year = set()
    for item in watchlist_shows:
        watchlist_guids.update(extract_guid_set(item))
        watchlist_title_year.add((str(getattr(item, "title", "")).strip().lower(), getattr(item, "year", None)))

    filtered = []
    for show in shows:
        local_guids = extract_guid_set(show)
        if local_guids and local_guids.intersection(watchlist_guids):
            filtered.append(show)
            continue
        local_title_year = (str(getattr(show, "title", "")).strip().lower(), getattr(show, "year", None))
        if local_title_year in watchlist_title_year:
            filtered.append(show)

    note = (
        "Watchlist mode enabled: "
        f"{len(filtered)} matching TV shows found in library (watchlist has {len(watchlist_shows)} TV items)."
    )
    return filtered, note


def build_plex_signature(show):
    guid_values = sorted(
        [getattr(guid, "id", "") or str(guid) for guid in (getattr(show, "guids", []) or [])]
    )
    return f"{show.title}|{show.year}|{'|'.join(guid_values)}"


def get_or_create_cache_entry(cache, show):
    key = show_cache_key(show)
    entry = cache.get(key, {})
    entry["title"] = show.title
    entry["year"] = show.year
    entry["rating_key"] = key
    entry["plex_signature"] = build_plex_signature(show)
    entry.setdefault("ignored_missing", [])
    entry.setdefault("ignore_all_missing", False)
    cache[key] = entry
    return key, entry


def connect_library(config):
    try:
        plex = PlexServer(config.get("plex_base_url", ""), config.get("plex_token", ""))
        section = plex.library.section(config.get("library_name", ""))
        return plex, section, None
    except Exception as exc:
        return None, None, str(exc)


def tmdb_get(path, config, params=None):
    tmdb_api_key = config.get("tmdb_api_key", "")
    if not tmdb_api_key:
        return None, None

    request_params = {"api_key": tmdb_api_key}
    if params:
        request_params.update(params)

    try:
        response = HTTP_SESSION.get(
            f"{TMDB_BASE_URL}{path}", params=request_params, timeout=REQUEST_TIMEOUT
        )
        if response.status_code != 200:
            return response.status_code, None
        return 200, response.json()
    except requests.RequestException:
        return None, None


def get_plex_guid_ids(show):
    tmdb_id = None
    imdb_id = None
    for guid in getattr(show, "guids", []) or []:
        raw = getattr(guid, "id", "") or str(guid)
        if raw.startswith("tmdb://"):
            try:
                tmdb_id = int(raw.split("://", 1)[1])
            except ValueError:
                pass
        if raw.startswith("imdb://"):
            imdb_id = raw.split("://", 1)[1]
    return tmdb_id, imdb_id


def resolve_tmdb_from_imdb(imdb_id, config):
    if not imdb_id:
        return None
    status_code, data = tmdb_get(f"/find/{imdb_id}", config, {"external_source": "imdb_id"})
    if status_code != 200 or not data:
        return None
    tv_results = data.get("tv_results", [])
    if not tv_results:
        return None
    return tv_results[0].get("id")


def score_tmdb_candidate(show_title, plex_year, candidate):
    title_norm = normalize_title(show_title)
    names = [
        normalize_title(candidate.get("name", "")),
        normalize_title(candidate.get("original_name", "")),
    ]
    valid_names = [n for n in names if n]
    if not valid_names:
        return -1
    name_score = max(SequenceMatcher(None, title_norm, n).ratio() for n in valid_names)
    score = name_score * 100

    first_air = candidate.get("first_air_date") or ""
    candidate_year = None
    if len(first_air) >= 4 and first_air[:4].isdigit():
        candidate_year = int(first_air[:4])
    if plex_year and candidate_year:
        year_diff = abs(candidate_year - plex_year)
        if year_diff == 0:
            score += 20
        elif year_diff == 1:
            score += 10
        elif year_diff > 3:
            score -= 10

    popularity = candidate.get("popularity") or 0
    score += min(float(popularity), 50) / 10
    return score


def search_tmdb_id(show_title, plex_year, config):
    search_variants = [show_title]
    stripped = re.sub(r"\(.*?\)", "", show_title).strip()
    if stripped and stripped not in search_variants:
        search_variants.append(stripped)

    best = None
    best_score = -1
    year_candidates = [plex_year, plex_year - 1, plex_year + 1] if isinstance(plex_year, int) else [None]
    year_candidates = [y for y in year_candidates if y]

    for query in search_variants:
        for year in year_candidates + [None]:
            params = {"query": query}
            if year:
                params["first_air_date_year"] = year
            status_code, data = tmdb_get("/search/tv", config, params)
            if status_code != 200 or not data:
                continue
            for candidate in data.get("results", []):
                cid = candidate.get("id")
                if not cid:
                    continue
                score = score_tmdb_candidate(show_title, plex_year, candidate)
                if score > best_score:
                    best_score = score
                    best = cid

    return best


def resolve_tmdb_id(show, cache_entry, config):
    manual_tmdb_id = cache_entry.get("manual_tmdb_id")
    if isinstance(manual_tmdb_id, int):
        return manual_tmdb_id, "manual"

    plex_tmdb_id, plex_imdb_id = get_plex_guid_ids(show)
    if isinstance(plex_tmdb_id, int):
        return plex_tmdb_id, "plex_guid"

    if plex_imdb_id:
        tmdb_from_imdb = resolve_tmdb_from_imdb(plex_imdb_id, config)
        if isinstance(tmdb_from_imdb, int):
            return tmdb_from_imdb, "plex_imdb"

    cached_tmdb_id = cache_entry.get("tmdb_id")
    if isinstance(cached_tmdb_id, int):
        status_code, _ = tmdb_get(f"/tv/{cached_tmdb_id}", config)
        if status_code == 200:
            return cached_tmdb_id, "cache"

    discovered = search_tmdb_id(show.title, show.year, config)
    if isinstance(discovered, int):
        return discovered, "auto"
    return None, "none"


def apply_ignored_missing(entry, missing_codes):
    if entry.get("ignore_all_missing"):
        return []
    ignored = set(entry.get("ignored_missing", []))
    return [code for code in missing_codes if code not in ignored]


def parse_iso_date(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def normalize_upcoming_list(entry):
    raw_items = []
    upcoming_all = entry.get("upcoming_all")
    if isinstance(upcoming_all, list):
        raw_items.extend(upcoming_all)
    next_air = entry.get("next_air")
    if isinstance(next_air, dict):
        raw_items.append(next_air)

    normalized = []
    seen = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        date_str = item.get("date")
        code = item.get("code")
        if not isinstance(code, str):
            continue
        if parse_iso_date(date_str) is None:
            continue
        token = (date_str, code)
        if token in seen:
            continue
        seen.add(token)
        normalized.append({"date": date_str, "code": code})

    normalized.sort(key=lambda x: (x["date"], x["code"]))
    return normalized


def advance_cached_missing_by_date(cache, today=None):
    check_date = today or datetime.now().date()
    changed = False

    for _, entry in cache.items():
        if not isinstance(entry, dict):
            continue

        upcoming_items = normalize_upcoming_list(entry)
        due_codes = []
        future_items = []

        for item in upcoming_items:
            air_date = parse_iso_date(item["date"])
            if air_date is None:
                continue
            if air_date <= check_date:
                due_codes.append(item["code"])
            else:
                future_items.append(item)

        existing_missing_raw = entry.get("missing_raw", entry.get("missing", [])) or []
        if not isinstance(existing_missing_raw, list):
            existing_missing_raw = []
        existing_missing_raw = [code for code in existing_missing_raw if isinstance(code, str)]

        merged_missing_raw = sorted(set(existing_missing_raw + due_codes))
        visible_missing = apply_ignored_missing(entry, merged_missing_raw)
        next_air = future_items[0] if future_items else None

        if entry.get("missing_raw") != merged_missing_raw:
            entry["missing_raw"] = merged_missing_raw
            changed = True
        if entry.get("missing") != visible_missing:
            entry["missing"] = visible_missing
            changed = True
        if entry.get("upcoming_all") != future_items:
            entry["upcoming_all"] = future_items
            changed = True
        if entry.get("next_air") != next_air:
            entry["next_air"] = next_air
            changed = True

    return changed


def build_scan_key_list(shows, cache, scan_mode="full"):
    mode = "incremental" if scan_mode == "incremental" else "full"
    if mode == "full":
        return [str(show.ratingKey) for show in shows]

    keys = []
    for show in shows:
        key = str(show.ratingKey)
        entry = cache.get(key)
        if not isinstance(entry, dict):
            keys.append(key)
            continue
        if entry.get("force_rescan"):
            keys.append(key)
            continue
        if not isinstance(entry.get("tmdb_id"), int):
            keys.append(key)
            continue
        if entry.get("missing"):
            keys.append(key)
            continue
        if entry.get("status") not in ENDED_STATUSES:
            keys.append(key)
            continue

    return keys


def build_cached_refresh_key_list(cache):
    keys = []
    for key, entry in cache.items():
        if not isinstance(entry, dict):
            continue
        if not entry.get("title"):
            continue
        if entry.get("status") not in ENDED_STATUSES:
            keys.append(str(key))
            continue
        if entry.get("missing"):
            keys.append(str(key))
            continue
        if not isinstance(entry.get("tmdb_id"), int):
            keys.append(str(key))
            continue
    return sorted(set(keys))


def normalize_episode_code(season_number, episode_number):
    if not isinstance(season_number, int) or not isinstance(episode_number, int):
        return None
    return f"S{season_number:02d}E{episode_number:02d}"


def parse_episode_code(code):
    if not isinstance(code, str):
        return None, None
    match = re.match(r"^S(\d{2})E(\d{2})$", code)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def remove_resolved_missing(raw_missing, local_episodes):
    cleaned = []
    for code in raw_missing:
        s_num, e_num = parse_episode_code(code)
        if s_num is None or e_num is None:
            continue
        if e_num in local_episodes.get(s_num, []):
            continue
        cleaned.append(code)
    return sorted(set(cleaned))


def build_season_cache_key(tmdb_id, season_number, freshness_token):
    token = freshness_token or "unknown"
    return f"{tmdb_id}:{season_number}:{token}"


def load_cached_tmdb_season(tmdb_id, season_number, freshness_token, season_cache):
    key = build_season_cache_key(tmdb_id, season_number, freshness_token)
    data = season_cache.get(key)
    if isinstance(data, dict):
        return data
    return None


def store_cached_tmdb_season(tmdb_id, season_number, freshness_token, data, season_cache):
    key = build_season_cache_key(tmdb_id, season_number, freshness_token)
    season_cache[key] = data


def compact_tmdb_season_payload(season_data):
    if not isinstance(season_data, dict):
        return None
    episodes = []
    for ep in season_data.get("episodes", []):
        if not isinstance(ep, dict):
            continue
        e_num = ep.get("episode_number")
        air_date = ep.get("air_date")
        if not isinstance(e_num, int):
            continue
        if not isinstance(air_date, str) or parse_iso_date(air_date) is None:
            continue
        episodes.append({"episode_number": e_num, "air_date": air_date})
    episodes.sort(key=lambda x: x["episode_number"])
    return {"episodes": episodes}


def get_tmdb_season_data(tmdb_id, season_number, config, freshness_token, season_cache):
    cached = load_cached_tmdb_season(tmdb_id, season_number, freshness_token, season_cache)
    if cached is not None:
        compact_cached = compact_tmdb_season_payload(cached)
        if compact_cached is not None:
            if compact_cached != cached:
                store_cached_tmdb_season(
                    tmdb_id, season_number, freshness_token, compact_cached, season_cache
                )
            return compact_cached
    status_code, s_data = tmdb_get(f"/tv/{tmdb_id}/season/{season_number}", config)
    if status_code != 200 or not s_data:
        return None
    compact_data = compact_tmdb_season_payload(s_data)
    if compact_data is None:
        return None
    store_cached_tmdb_season(tmdb_id, season_number, freshness_token, compact_data, season_cache)
    return compact_data


def collect_missing_and_upcoming_from_season_data(
    season_data, season_number, local_episodes, today, raw_missing, upcoming_items
):
    if not isinstance(season_data, dict):
        return

    for ep in season_data.get("episodes", []):
        air_date_str = ep.get("air_date")
        if not air_date_str:
            continue
        try:
            air_date = datetime.strptime(air_date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        e_num = ep.get("episode_number")
        if not isinstance(e_num, int):
            continue
        code = normalize_episode_code(season_number, e_num)
        if code is None:
            continue

        if air_date <= today:
            if e_num not in local_episodes.get(season_number, []):
                raw_missing.append(code)
        else:
            upcoming_items.append({"date": str(air_date), "code": code})


def evaluate_show(show, cache_entry, config, today, deep_audit=False):
    show_title = show.title
    show_year = show.year

    if (
        cache_entry.get("status") in ENDED_STATUSES
        and not cache_entry.get("missing")
        and cache_entry.get("tmdb_id")
        and cache_entry.get("imdb_id")
        and not cache_entry.get("force_rescan")
    ):
        return (
            {
                "cache_key": cache_entry["rating_key"],
                "title": show_title,
                "year": show_year,
                "status": cache_entry.get("status"),
                "tmdb_id": cache_entry.get("tmdb_id"),
                "imdb_id": cache_entry.get("imdb_id"),
                "missing": [],
                "next_air": None,
                "tmdb_source": cache_entry.get("tmdb_source", "cache"),
                "poster_url": cache_entry.get("poster_url"),
            },
            None,
        )

    tmdb_id, source = resolve_tmdb_id(show, cache_entry, config)
    if not tmdb_id:
        cache_entry["last_scan_at"] = datetime.now().isoformat(timespec="seconds")
        return None, f"{show_title} ({show_year})"

    status_code, details = tmdb_get(
        f"/tv/{tmdb_id}", config, {"append_to_response": "external_ids"}
    )
    if status_code != 200 or not details:
        cache_entry["last_scan_at"] = datetime.now().isoformat(timespec="seconds")
        return None, f"{show_title} ({show_year})"

    status = details.get("status", "Unknown")
    poster_path = details.get("poster_path")
    poster_url = f"{TMDB_IMAGE_BASE_URL}{poster_path}" if poster_path else None
    imdb_id = (details.get("external_ids") or {}).get("imdb_id")
    if not imdb_id:
        status_code, external_ids_data = tmdb_get(f"/tv/{tmdb_id}/external_ids", config)
        if status_code == 200 and external_ids_data:
            imdb_id = external_ids_data.get("imdb_id")

    local_episodes = {s.index: [e.index for e in s.episodes()] for s in show.seasons()}
    cached_raw_missing = cache_entry.get("missing_raw", cache_entry.get("missing", [])) or []
    if not isinstance(cached_raw_missing, list):
        cached_raw_missing = []
    raw_missing = remove_resolved_missing(
        [code for code in cached_raw_missing if isinstance(code, str)], local_episodes
    )

    upcoming_items = normalize_upcoming_list({"upcoming_all": cache_entry.get("upcoming_all", [])})
    if not upcoming_items and isinstance(cache_entry.get("next_air"), dict):
        upcoming_items = normalize_upcoming_list({"next_air": cache_entry.get("next_air")})

    fresh_upcoming = []
    next_ep = details.get("next_episode_to_air") or {}
    next_date = parse_iso_date(next_ep.get("air_date"))
    next_code = normalize_episode_code(next_ep.get("season_number"), next_ep.get("episode_number"))
    if next_date and next_code and next_date > today:
        fresh_upcoming.append({"date": str(next_date), "code": next_code})

    last_ep = details.get("last_episode_to_air") or {}
    last_ep_date = parse_iso_date(last_ep.get("air_date"))
    last_ep_season = last_ep.get("season_number")
    last_ep_number = last_ep.get("episode_number")
    last_ep_code = normalize_episode_code(last_ep_season, last_ep_number)
    if last_ep_date and last_ep_date <= today and last_ep_code:
        if last_ep_number not in local_episodes.get(last_ep_season, []):
            raw_missing.append(last_ep_code)

    tmdb_freshness_token = details.get("last_air_date") or (
        str(next_date) if next_date else ""
    )
    season_cache = load_tmdb_season_cache()
    season_cache_changed = False

    should_probe_current_season = False
    if isinstance(last_ep_season, int) and isinstance(last_ep_number, int):
        max_local = max(local_episodes.get(last_ep_season, [0]) or [0])
        if max_local + 1 < last_ep_number:
            should_probe_current_season = True

    if deep_audit:
        seasons_to_scan = []
        for season in details.get("seasons", []):
            s_num = season.get("season_number")
            if s_num in (None, 0):
                continue
            if s_num > max(local_episodes.keys(), default=0) + 1:
                continue
            seasons_to_scan.append(s_num)
    else:
        seasons_to_scan = []
        if should_probe_current_season and isinstance(last_ep_season, int) and last_ep_season > 0:
            seasons_to_scan.append(last_ep_season)

    for s_num in sorted(set(seasons_to_scan)):
        cached_before = load_cached_tmdb_season(
            tmdb_id, s_num, tmdb_freshness_token, season_cache
        )
        s_data = get_tmdb_season_data(tmdb_id, s_num, config, tmdb_freshness_token, season_cache)
        if s_data is None:
            continue
        cached_after = load_cached_tmdb_season(
            tmdb_id, s_num, tmdb_freshness_token, season_cache
        )
        if cached_before is None or cached_after != cached_before:
            season_cache_changed = True
        collect_missing_and_upcoming_from_season_data(
            s_data, s_num, local_episodes, today, raw_missing, fresh_upcoming
        )

    if season_cache_changed:
        save_tmdb_season_cache(season_cache)

    raw_missing = sorted(set(raw_missing))
    upcoming = normalize_upcoming_list({"upcoming_all": upcoming_items})
    upcoming = normalize_upcoming_list({"upcoming_all": upcoming + fresh_upcoming})
    next_air = upcoming[0] if upcoming else None
    visible_missing = apply_ignored_missing(cache_entry, raw_missing)

    cache_entry["tmdb_id"] = tmdb_id
    cache_entry["imdb_id"] = imdb_id
    cache_entry["tmdb_source"] = source
    cache_entry["poster_url"] = poster_url
    cache_entry["status"] = status
    cache_entry["missing_raw"] = raw_missing
    cache_entry["missing"] = visible_missing
    cache_entry["upcoming_all"] = upcoming
    cache_entry["next_air"] = next_air
    cache_entry["force_rescan"] = False
    cache_entry["last_scan_at"] = datetime.now().isoformat(timespec="seconds")

    return (
        {
            "cache_key": cache_entry["rating_key"],
            "title": show_title,
            "year": show_year,
            "status": status,
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "missing": visible_missing,
            "next_air": next_air,
            "upcoming_all": upcoming,
            "tmdb_source": source,
            "poster_url": poster_url,
        },
        None,
    )


def reconcile_cache_with_library(cache, shows):
    active_keys = {str(show.ratingKey) for show in shows}
    removed = 0
    changed = 0

    for key in list(cache.keys()):
        if key not in active_keys:
            del cache[key]
            removed += 1

    for show in shows:
        key = str(show.ratingKey)
        entry = cache.get(key)
        if not entry:
            continue
        new_signature = build_plex_signature(show)
        if entry.get("plex_signature") != new_signature:
            changed += 1
            entry["title"] = show.title
            entry["year"] = show.year
            entry["plex_signature"] = new_signature
            if "manual_tmdb_id" not in entry:
                entry.pop("tmdb_id", None)
                entry.pop("imdb_id", None)
                entry.pop("tmdb_source", None)
                entry.pop("status", None)
                entry.pop("next_air", None)
                entry.pop("upcoming_all", None)
                entry.pop("missing_raw", None)
                entry["missing"] = []
            entry["force_rescan"] = True
            cache[key] = entry

    return removed, changed


def init_scan_state(show_keys, cache, deep_audit=False, scan_mode="full"):
    return {
        "running": True,
        "paused": False,
        "cancel_requested": False,
        "index": 0,
        "total": len(show_keys),
        "show_keys": show_keys,
        "scan_mode": scan_mode,
        "results_map": {},
        "unmatched": [],
        "last_status": "Starting scan...",
        "cache": cache,
        "deep_audit": bool(deep_audit),
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "error": None,
    }


def process_scan_batch(state, config, batch_size=SCAN_BATCH_SIZE):
    if not state["running"] or state["paused"] or state["cancel_requested"]:
        return state

    plex, _, err = connect_library(config)
    if err:
        state["running"] = False
        state["error"] = "Failed to connect during scan: " + err
        save_cache(state["cache"])
        return state

    today = datetime.now().date()
    processed = 0

    while processed < batch_size and state["index"] < state["total"]:
        if state["cancel_requested"] or state["paused"]:
            break

        rating_key = state["show_keys"][state["index"]]
        try:
            show = plex.fetchItem(f"/library/metadata/{rating_key}")
            cache_key, cache_entry = get_or_create_cache_entry(state["cache"], show)
            result, unmatched = evaluate_show(
                show,
                cache_entry,
                config,
                today,
                deep_audit=bool(state.get("deep_audit", False)),
            )
            state["cache"][cache_key] = cache_entry

            if result is not None:
                state["results_map"][cache_key] = result
            if unmatched:
                state["unmatched"].append(unmatched)

            state["last_status"] = f"Checked: {show.title} ({show.year})"
        except Exception as exc:
            state["unmatched"].append(f"ratingKey={rating_key} ({exc})")

        state["index"] += 1
        processed += 1

    if state["cancel_requested"]:
        state["running"] = False
        state["paused"] = False
        state["last_status"] = "Scan cancelled"
        save_cache(state["cache"])
        return state

    if state["index"] >= state["total"]:
        state["running"] = False
        state["paused"] = False
        state["last_status"] = "Scan complete"
        save_cache(state["cache"])
        if state.get("scan_mode") == "full":
            mark_full_scan_completed()

    return state


def refresh_single_show(cache_key, config, results_map=None):
    missing = validate_config(config)
    if missing:
        return False, "Missing config values: " + ", ".join(missing), None

    cache = load_cache()
    entry = cache.get(cache_key)
    if not entry:
        return False, "Show not found in cache.", None

    plex, _, err = connect_library(config)
    if err:
        return False, "Failed to connect to Plex: " + err, None

    try:
        show = plex.fetchItem(f"/library/metadata/{cache_key}")
    except Exception as exc:
        return False, f"Failed to load show from Plex: {exc}", None

    _, cache_entry = get_or_create_cache_entry(cache, show)
    result, unmatched = evaluate_show(show, cache_entry, config, datetime.now().date(), deep_audit=False)
    cache[cache_key] = cache_entry
    save_cache(cache)

    if results_map is not None and result is not None:
        results_map[cache_key] = result

    if unmatched:
        return False, f"TMDB mapping still unresolved for {unmatched}.", result
    return True, "Show refreshed with latest TMDB/IMDb mapping.", result


def set_tmdb_override(cache_key, tmdb_value, config, results_map=None):
    cache = load_cache()
    entry = cache.get(cache_key)
    if not entry:
        return False, "Show not found in cache."

    if tmdb_value is None:
        entry.pop("manual_tmdb_id", None)
        entry.pop("tmdb_id", None)
        entry.pop("imdb_id", None)
        entry.pop("tmdb_source", None)
        entry.pop("status", None)
        entry.pop("next_air", None)
        entry.pop("upcoming_all", None)
        entry.pop("poster_url", None)
        entry.pop("missing_raw", None)
        entry["missing"] = []
        entry["force_rescan"] = True
        cache[cache_key] = entry
        save_cache(cache)
        refreshed, refresh_msg, _ = refresh_single_show(cache_key, config, results_map)
        if refreshed:
            return True, "TMDB override cleared and show remapped."
        return False, "TMDB override cleared, but refresh failed: " + refresh_msg

    try:
        tmdb_id = int(str(tmdb_value).strip())
    except ValueError:
        return False, "TMDB ID must be an integer."

    status_code, _ = tmdb_get(f"/tv/{tmdb_id}", config)
    if status_code != 200:
        return False, "TMDB ID is invalid or not reachable."

    entry["manual_tmdb_id"] = tmdb_id
    entry["tmdb_id"] = tmdb_id
    entry.pop("imdb_id", None)
    entry.pop("status", None)
    entry.pop("next_air", None)
    entry.pop("upcoming_all", None)
    entry.pop("poster_url", None)
    entry.pop("missing_raw", None)
    entry["missing"] = []
    entry["force_rescan"] = True
    entry["tmdb_source"] = "manual"
    cache[cache_key] = entry
    save_cache(cache)
    refreshed, refresh_msg, _ = refresh_single_show(cache_key, config, results_map)
    if refreshed:
        return True, "TMDB override saved and show refreshed."
    return False, "TMDB override saved, but refresh failed: " + refresh_msg


def set_episode_ignore(cache_key, episode_code, ignore=True):
    cache = load_cache()
    entry = cache.get(cache_key)
    if not entry:
        return

    ignored = set(entry.get("ignored_missing", []))
    if ignore:
        ignored.add(episode_code)
    else:
        ignored.discard(episode_code)

    entry["ignored_missing"] = sorted(ignored)
    if "missing" in entry:
        entry["missing"] = [code for code in entry.get("missing", []) if code not in ignored]
    cache[cache_key] = entry
    save_cache(cache)


def set_show_missing_ignore(cache_key, ignore=True):
    cache = load_cache()
    entry = cache.get(cache_key)
    if not entry:
        return

    entry["ignore_all_missing"] = bool(ignore)
    if ignore:
        entry["missing"] = []
    else:
        base_missing = entry.get("missing_raw", entry.get("missing", []))
        entry["missing"] = apply_ignored_missing(entry, base_missing)
    cache[cache_key] = entry
    save_cache(cache)


def apply_overrides_to_results(results):
    cache = load_cache()
    output = []
    for item in results:
        entry = cache.get(item["cache_key"], {})
        source_missing = entry.get("missing", item.get("missing", [])) or []
        visible_missing = apply_ignored_missing(entry, source_missing)
        updated = item.copy()
        updated["missing"] = visible_missing
        updated["tmdb_id"] = entry.get("tmdb_id")
        updated["imdb_id"] = entry.get("imdb_id")
        updated["tmdb_source"] = entry.get("tmdb_source", updated.get("tmdb_source"))
        updated["status"] = entry.get("status", updated.get("status"))
        updated["next_air"] = entry.get("next_air", updated.get("next_air"))
        updated["upcoming_all"] = entry.get("upcoming_all", updated.get("upcoming_all", []))
        updated["poster_url"] = entry.get("poster_url", updated.get("poster_url"))
        output.append(updated)
    return output


def extract_season_number(code):
    match = re.match(r"^S(\d{2})E\d{2}$", code)
    if not match:
        return None
    return int(match.group(1))


def _normalize_base_url(value):
    return str(value or "").strip().rstrip("/")


def _normalize_imdb_for_newznab(imdb_id):
    if not isinstance(imdb_id, str):
        return None
    token = imdb_id.strip()
    if not token:
        return None
    lower = token.lower()
    if lower.startswith("tt") and token[2:].isdigit():
        return token[2:]
    return token


def _build_indexer_missing_link(item, missing_code, season_num, episode_num, config):
    provider = normalize_indexer_provider(
        config.get("indexer_provider", config.get("usenet_provider"))
    )
    if provider == "none":
        return None

    query = f"{item.get('title', '')} {missing_code}".strip()
    api_key = str(config.get("indexer_api_key", config.get("usenet_api_key", "")) or "").strip()
    imdb_id = _normalize_imdb_for_newznab(item.get("imdb_id"))
    provider_labels = {
        "nzbhydra": "NZBHydra",
        "newznab": "Newznab",
        "torznab": "Torznab",
        "prowlarr": "Prowlarr",
        "torbox": "TorBox",
        "jackett": "Jackett",
        "custom": "Custom",
    }
    provider_label = provider_labels.get(provider, provider.title())
    web_template = str(config.get("indexer_web_url_template", config.get("usenet_web_url_template", "")) or "").strip()
    if not web_template:
        return None

    supported_tokens = {
        "{query}",
        "{query_url}",
        "{title}",
        "{title_url}",
        "{code}",
        "{code_url}",
        "{season}",
        "{episode}",
        "{imdbid}",
    }
    if not any(token in web_template for token in supported_tokens):
        # Convenience fallback: if user provides only a base URL (or static URL),
        # append a query key so each episode opens a distinct search.
        sep = "&" if "?" in web_template else "?"
        provider_query_key = {
            "nzbhydra": "query",
            "prowlarr": "query",
            "torbox": "query",
            "newznab": "q",
            "torznab": "q",
            "jackett": "q",
            "custom": "q",
        }
        query_key = provider_query_key.get(provider, "q")
        api_key_suffix = f"&apikey={urlencode({'k': api_key})[2:]}" if api_key else ""
        return {
            "label": f"{missing_code} - Open {provider_label} Web Search",
            "url": f"{web_template}{sep}{query_key}={urlencode({'q': query})[2:]}{api_key_suffix}",
        }

    replacements = {
        "{query}": query,
        "{title}": str(item.get("title", "") or ""),
        "{code}": missing_code,
        "{season}": str(season_num) if isinstance(season_num, int) else "",
        "{episode}": str(episode_num) if isinstance(episode_num, int) else "",
        "{imdbid}": str(imdb_id or ""),
        "{apikey}": api_key,
    }
    resolved_url = web_template
    for token, value in replacements.items():
        resolved_url = resolved_url.replace(token, value)
    if "{query_url}" in resolved_url:
        resolved_url = resolved_url.replace("{query_url}", urlencode({"q": query})[2:])
    if "{title_url}" in resolved_url:
        resolved_url = resolved_url.replace(
            "{title_url}", urlencode({"q": str(item.get("title", "") or "")})[2:]
        )
    if "{code_url}" in resolved_url:
        resolved_url = resolved_url.replace("{code_url}", urlencode({"q": missing_code})[2:])

    return {
        "label": f"{missing_code} - Open {provider_label} Web Search",
        "url": resolved_url,
    }


def _build_usenet_missing_link(item, missing_code, season_num, episode_num, config):
    # Backward compatible alias.
    return _build_indexer_missing_link(item, missing_code, season_num, episode_num, config)


def build_missing_episode_links(item, missing_code, config):
    links = []
    season_num = extract_season_number(missing_code)
    dmm_enabled = normalize_bool_flag(config.get("include_dmm_link"), default=True)
    dmm_base_url = _normalize_base_url(config.get("dmm_base_url") or "https://debridmediamanager.com")
    imdb_id = item.get("imdb_id")
    if dmm_enabled and season_num is not None and imdb_id and dmm_base_url:
        links.append(
            {
                "label": f"{missing_code} - Open season in DMM",
                "url": f"{dmm_base_url}/show/{imdb_id}/{season_num}",
            }
        )

    parsed_season, parsed_episode = parse_episode_code(missing_code)
    indexer_link = _build_indexer_missing_link(
        item, missing_code, parsed_season, parsed_episode, config
    )
    if indexer_link:
        links.append(indexer_link)

    return links


def run_git_command(args, timeout=20):
    if not shutil.which("git"):
        return False, "", "Git is not installed or not available in PATH."

    try:
        result = subprocess.run(
            ["git"] + args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except Exception as exc:
        return False, "", str(exc)

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        return False, stdout, stderr or "Git command failed."
    return True, stdout, stderr


def check_app_updates(fetch_remote=True):
    ok, _, err = run_git_command(["rev-parse", "--is-inside-work-tree"])
    if not ok:
        return {
            "ok": False,
            "message": "Update check is available only when running from a git repository.",
            "error": err,
        }

    ok, branch, err = run_git_command(["rev-parse", "--abbrev-ref", "HEAD"])
    if not ok:
        return {"ok": False, "message": "Failed to detect current branch.", "error": err}
    if branch == "HEAD":
        return {
            "ok": False,
            "message": "Detached HEAD detected. Switch to a branch to check updates.",
            "error": "",
        }

    ok, local_sha, err = run_git_command(["rev-parse", "HEAD"])
    if not ok:
        return {"ok": False, "message": "Failed to read local commit.", "error": err}

    ok, remote_url, err = run_git_command(["remote", "get-url", "origin"])
    if not ok:
        return {
            "ok": False,
            "message": "Git remote 'origin' is missing. Cannot check remote updates.",
            "error": err,
        }

    if fetch_remote:
        ok, _, err = run_git_command(["fetch", "origin", branch, "--quiet"], timeout=45)
        if not ok:
            return {"ok": False, "message": "Failed to fetch remote updates.", "error": err}

    ok, remote_sha, err = run_git_command(["rev-parse", f"origin/{branch}"])
    if not ok:
        return {
            "ok": False,
            "message": f"Remote branch origin/{branch} not found.",
            "error": err,
        }

    ok, behind_raw, err = run_git_command(["rev-list", "--count", f"HEAD..origin/{branch}"])
    if not ok:
        return {"ok": False, "message": "Failed to calculate update distance.", "error": err}
    ok, ahead_raw, err = run_git_command(["rev-list", "--count", f"origin/{branch}..HEAD"])
    if not ok:
        return {"ok": False, "message": "Failed to calculate update distance.", "error": err}

    behind = int(behind_raw or "0")
    ahead = int(ahead_raw or "0")
    up_to_date = behind == 0

    if up_to_date and ahead == 0:
        message = "App is up to date."
    elif behind > 0 and ahead == 0:
        message = f"Update available: {behind} commit(s) behind origin/{branch}."
    elif behind == 0 and ahead > 0:
        message = f"Local branch is {ahead} commit(s) ahead of origin/{branch}."
    else:
        message = f"Branch has diverged ({ahead} ahead, {behind} behind)."

    return {
        "ok": True,
        "message": message,
        "branch": branch,
        "remote_url": remote_url,
        "local_sha": local_sha,
        "remote_sha": remote_sha,
        "behind": behind,
        "ahead": ahead,
    }


def update_app_from_remote(auto_stash=True):
    status = check_app_updates(fetch_remote=True)
    if not status.get("ok"):
        return False, status.get("message", "Update check failed."), status

    branch = status.get("branch")
    behind = int(status.get("behind", 0) or 0)
    ahead = int(status.get("ahead", 0) or 0)

    if behind == 0:
        return True, "Already up to date.", status
    if ahead > 0:
        return False, "Local branch is ahead/diverged. Update skipped to avoid conflicts.", status

    ok, dirty_out, dirty_err = run_git_command(["status", "--porcelain"])
    if not ok:
        return False, "Failed to check local git working tree state.", {"error": dirty_err, **status}

    dirty = bool(dirty_out.strip())
    stashed = False
    if dirty:
        if not auto_stash:
            return (
                False,
                "Update blocked: you have local uncommitted changes. Commit or stash them, then try again.",
                {"error": "working tree is dirty", **status},
            )
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ok, stash_out, stash_err = run_git_command(
            ["stash", "push", "--include-untracked", "-m", f"Plex Parity auto-stash before update ({stamp})"],
            timeout=60,
        )
        if not ok:
            return False, "Update failed: could not stash local changes.", {"error": stash_err, **status}
        stashed = True

    ok, _, err = run_git_command(["pull", "--ff-only", "origin", branch], timeout=60)
    if not ok:
        if stashed:
            run_git_command(["stash", "pop"], timeout=60)
        detail = err.strip() if isinstance(err, str) else ""
        if detail:
            return (
                False,
                f"Update failed. Fast-forward pull was not possible. Git says: {detail}",
                {"error": detail, **status},
            )
        return False, "Update failed. Fast-forward pull was not possible.", {"error": err, **status}

    if stashed:
        ok, pop_out, pop_err = run_git_command(["stash", "pop"], timeout=60)
        if not ok:
            detail = pop_err.strip() if isinstance(pop_err, str) else ""
            msg = (
                "Update completed, but restoring your stashed local changes failed. "
                "Resolve manually with `git stash list` / `git stash pop`."
            )
            return False, msg, {"error": detail or pop_err, **status}

    refreshed = check_app_updates(fetch_remote=False)
    if stashed:
        return True, "Update completed (auto-stash applied). Restart app to load changes.", refreshed
    return True, "Update completed. Restart the app to ensure all changes are loaded.", refreshed
