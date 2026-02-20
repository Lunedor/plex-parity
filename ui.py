from datetime import datetime

import streamlit as st

from backend import (
    ENDED_STATUSES,
    advance_cached_missing_by_date,
    apply_overrides_to_results,
    build_missing_episode_links,
    build_cached_refresh_key_list,
    build_scan_key_list,
    check_app_updates,
    cache_has_show_entries,
    connect_library,
    filter_shows_for_scan,
    init_scan_state,
    has_full_scan_completed,
    load_cache,
    load_config,
    load_results_map_from_cache,
    process_scan_batch,
    reconcile_cache_with_library,
    refresh_single_show,
    save_cache,
    save_config,
    set_episode_ignore,
    set_show_missing_ignore,
    set_tmdb_override,
    update_app_from_remote,
    validate_config,
)


def ensure_scan_state():
    if "scan_state" not in st.session_state:
        cache = load_cache()
        if advance_cached_missing_by_date(cache):
            save_cache(cache)
        cached_results = load_results_map_from_cache(cache)
        st.session_state["scan_state"] = {
            "running": False,
            "paused": False,
            "cancel_requested": False,
            "index": 0,
            "total": 0,
            "show_keys": [],
            "results_map": cached_results,
            "unmatched": [],
            "last_status": "Idle",
            "cache": cache,
            "started_at": None,
            "error": None,
        }


def trigger_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


def start_scan(config, scan_mode="full"):
    missing = validate_config(config)
    if missing:
        st.error("Missing config values: " + ", ".join(missing))
        return

    cache = load_cache()
    if advance_cached_missing_by_date(cache):
        save_cache(cache)
    previous_results = (
        st.session_state.get("scan_state", {}).get("results_map")
        or load_results_map_from_cache(cache)
    )

    removed = 0
    changed = 0

    if scan_mode == "refresh_cached":
        show_keys = build_cached_refresh_key_list(cache)
    else:
        plex, section, err = connect_library(config)
        if err:
            st.error("Failed to connect to Plex: " + err)
            return

        shows = section.all()
        scan_scope = config.get("scan_scope", "all_library")
        shows, scope_note = filter_shows_for_scan(plex, shows, scan_scope)
        if scope_note:
            st.info(scope_note)
        if not shows:
            st.warning("No shows found for current scan scope.")
            return

        removed, changed = reconcile_cache_with_library(cache, shows)
        show_keys = build_scan_key_list(shows, cache, scan_mode=scan_mode)

    if not show_keys:
        if scan_mode == "incremental":
            st.info("Incremental scan found nothing new/changed to process.")
        elif scan_mode == "refresh_cached":
            st.info("Cached refresh found nothing active to process.")
        else:
            st.info("No shows available to scan.")
        return
    new_state = init_scan_state(
        show_keys,
        cache,
        deep_audit=(scan_mode == "full"),
        scan_mode=scan_mode,
    )
    if scan_mode == "full":
        new_state["last_status"] = "Starting full scan..."
    elif scan_mode == "incremental":
        new_state["last_status"] = "Starting incremental scan..."
    else:
        new_state["last_status"] = "Refreshing cached ongoing data..."
    new_state["results_map"] = previous_results.copy()
    st.session_state["scan_state"] = new_state
    if scan_mode != "refresh_cached" and (removed > 0 or changed > 0):
        st.info(f"Cache reconciled: removed {removed} deleted shows, refreshed {changed} rematched shows.")
    trigger_rerun()


def get_results_map():
    ensure_scan_state()
    return st.session_state["scan_state"]["results_map"]


def apply_global_style():
    st.markdown(
        """
<style>
.block-container {
  max-width: 1200px;
  padding-top: 1.5rem;
}
h1, h2, h3 {
  letter-spacing: -0.02em;
}
.stTextInput input, .stTextArea textarea {
  color: inherit !important;
}
div[data-baseweb="select"] > div {
  color: inherit !important;
  background: rgba(127, 127, 127, 0.10) !important;
  border: 1px solid rgba(127, 127, 127, 0.45) !important;
}
div[data-baseweb="select"] input {
  color: inherit !important;
}
ul[role="listbox"] {
  background: var(--background-color, #1f1f1f) !important;
  border: 1px solid rgba(127, 127, 127, 0.45) !important;
}
ul[role="listbox"] li {
  color: inherit !important;
}
textarea, input[type="text"], input[type="password"] {
  background: rgba(127, 127, 127, 0.10) !important;
  border: 1px solid rgba(127, 127, 127, 0.45) !important;
}
.stButton > button {
  border: 1px solid rgba(127, 127, 127, 0.55) !important;
}
.upcoming-card {
  border: 1px solid rgba(127, 127, 127, 0.35);
  border-radius: 12px;
  padding: 0.7rem 0.8rem;
  background: rgba(127, 127, 127, 0.08);
  min-height: 136px;
  display: flex;
  gap: 0.8rem;
  align-items: center;
}
.upcoming-poster {
  width: 68px;
  height: 100px;
  object-fit: cover;
  border-radius: 8px;
  border: 1px solid rgba(127, 127, 127, 0.35);
  flex-shrink: 0;
}
.show-grid-card {
  border: 1px solid rgba(127, 127, 127, 0.35);
  border-radius: 12px;
  padding: 0.75rem 0.8rem;
  background: rgba(127, 127, 127, 0.06);
  margin-bottom: 0.8rem;
  display: flex;
  gap: 0.85rem;
  align-items: center;
}
.show-card-poster {
  width: 70px;
  height: 102px;
  object-fit: cover;
  border-radius: 8px;
  border: 1px solid rgba(127, 127, 127, 0.35);
  flex-shrink: 0;
}
.show-card-meta {
  width: 100%;
  text-align: center;
}
.upcoming-title {
  font-weight: 700;
  line-height: 1.2;
  margin-bottom: 0.35rem;
  word-break: break-word;
}
.upcoming-line {
  font-size: 0.92rem;
  line-height: 1.3;
  word-break: break-word;
}
.upcoming-eta {
  margin-top: 0.45rem;
  font-size: 0.85rem;
  font-weight: 600;
  opacity: 0.9;
}
.timeline-row {
  border: 1px solid rgba(127, 127, 127, 0.35);
  border-radius: 10px;
  padding: 0.55rem 0.7rem;
  margin-bottom: 0.4rem;
  background: rgba(127, 127, 127, 0.08);
  display: flex;
  justify-content: space-between;
  gap: 0.8rem;
}
.timeline-left {
  font-size: 0.92rem;
  line-height: 1.3;
  word-break: break-word;
}
.timeline-right {
  font-size: 0.82rem;
  font-weight: 700;
  opacity: 0.9;
  white-space: nowrap;
}
div[data-testid="stProgressBar"] > div {
  border-radius: 999px;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard_summary(data):
    total_shows = len(data)
    ongoing_missing = sum(1 for x in data if x.get("missing") and x.get("status") not in ENDED_STATUSES)
    archived_missing = sum(1 for x in data if x.get("missing") and x.get("status") in ENDED_STATUSES)
    upcoming_count = sum(1 for x in data if x.get("next_air"))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tracked Shows", total_shows)
    c2.metric("Ongoing Missing", ongoing_missing)
    c3.metric("Archive Missing", archived_missing)
    c4.metric("Upcoming", upcoming_count)


def render_scan_controls(config):
    st.subheader("Scan")
    ensure_scan_state()
    state = st.session_state["scan_state"]
    scope_label = (
        "Only Watchlisted TV Shows"
        if config.get("scan_scope", "all_library") == "watchlist_only"
        else "All Library Shows"
    )
    st.caption(f"Current scope: {scope_label}")
    current_mode = state.get("scan_mode", "idle")
    tmdb_mode_label = "Full audit" if state.get("deep_audit") else "Lightweight"
    if not state.get("running"):
        if current_mode == "full":
            tmdb_mode_label = "Full audit"
        elif current_mode in {"incremental", "refresh_cached"}:
            tmdb_mode_label = "Lightweight"
        else:
            tmdb_mode_label = "Lightweight"
    st.caption(f"TMDB call mode: {tmdb_mode_label}")
    quick_modes_ready = has_full_scan_completed() and cache_has_show_entries()
    if not quick_modes_ready:
        st.caption("Incremental and Refresh Cached unlock after one successful full scan with populated cache.")

    action_col1, action_col2, action_col3, action_col4, action_col5, action_col6 = st.columns([2, 2, 2, 1, 1, 1])
    with action_col1:
        if st.button("Start Full", disabled=state["running"], width="stretch"):
            start_scan(config, scan_mode="full")
    with action_col2:
        if st.button("Start Incremental", disabled=(state["running"] or not quick_modes_ready), width="stretch"):
            start_scan(config, scan_mode="incremental")
    with action_col3:
        if st.button("Refresh Cached", disabled=(state["running"] or not quick_modes_ready), width="stretch"):
            start_scan(config, scan_mode="refresh_cached")
    with action_col4:
        if st.button(
            "Pause",
            disabled=(not state["running"] or state["paused"]),
            width="stretch",
        ):
            state["paused"] = True
            state["last_status"] = "Paused"
            trigger_rerun()
    with action_col5:
        if st.button(
            "Resume",
            disabled=(not state["running"] or not state["paused"]),
            width="stretch",
        ):
            state["paused"] = False
            state["last_status"] = "Resuming"
            trigger_rerun()
    with action_col6:
        if st.button("Cancel", disabled=not state["running"], width="stretch"):
            state["cancel_requested"] = True
            state["last_status"] = "Cancelling..."
            trigger_rerun()

    progress_value = (state["index"] / state["total"]) if state["total"] > 0 else 0.0
    st.progress(progress_value)
    st.caption(f"{state['index']} / {state['total']} | {state['last_status']}")

    if state.get("error"):
        st.error(state["error"])

    if state["running"] and not state["paused"] and not state["cancel_requested"]:
        st.session_state["scan_state"] = process_scan_batch(state, config)
        trigger_rerun()


def render_tmdb_mapping_maintenance(config):
    st.subheader("TMDB Mapping Maintenance")
    cache = load_cache()
    if not cache:
        st.info("Cache is empty. Run a scan first.")
        return

    cache_items = sorted(
        cache.items(),
        key=lambda item: (item[1].get("title") or "").lower(),
    )
    labels = {
        key: f"{entry.get('title', 'Unknown')} ({entry.get('year', 'N/A')}) [tmdb={entry.get('tmdb_id', '-')}]"
        for key, entry in cache_items
    }
    all_keys = [key for key, _ in cache_items]
    pending_keys = [key for key, entry in cache_items if not isinstance(entry.get("tmdb_id"), int)]

    st.caption(f"Shows needing TMDB mapping: {len(pending_keys)}")
    if pending_keys:
        if (
            "maintenance_pending_key" not in st.session_state
            or st.session_state["maintenance_pending_key"] not in pending_keys
        ):
            st.session_state["maintenance_pending_key"] = pending_keys[0]

        pending_key = st.selectbox(
            "Unmapped shows",
            options=pending_keys,
            key="maintenance_pending_key",
            format_func=lambda key: labels[key],
        )
        if st.button("Edit Selected Unmapped Show", key="maintenance_edit_unmapped"):
            st.session_state["maintenance_selected_key"] = pending_key
            trigger_rerun()
    else:
        st.success("All cached shows have TMDB IDs.")

    if (
        "maintenance_selected_key" not in st.session_state
        or st.session_state["maintenance_selected_key"] not in all_keys
    ):
        st.session_state["maintenance_selected_key"] = pending_keys[0] if pending_keys else all_keys[0]

    selected_key = st.selectbox(
        "Select show",
        options=all_keys,
        key="maintenance_selected_key",
        format_func=lambda key: labels[key],
    )
    selected_entry = cache[selected_key]
    current_tmdb = selected_entry.get("manual_tmdb_id") or selected_entry.get("tmdb_id") or ""
    tmdb_input = st.text_input("TMDB ID override", value=str(current_tmdb), key=f"maintenance_tmdb_{selected_key}")

    save_col, clear_col = st.columns(2)
    with save_col:
        if st.button("Save override", key=f"maintenance_save_{selected_key}"):
            ok, msg = set_tmdb_override(selected_key, tmdb_input, config, get_results_map())
            if ok:
                st.success(msg)
                trigger_rerun()
            else:
                st.error(msg)
    with clear_col:
        if st.button("Clear override", key=f"maintenance_clear_{selected_key}"):
            ok, msg = set_tmdb_override(selected_key, None, config, get_results_map())
            if ok:
                st.success(msg)
                trigger_rerun()
            else:
                st.error(msg)


def render_config_editor(config):
    st.subheader("Configuration")
    indexer_provider_options = ["none", "newznab", "nzbhydra", "torznab", "prowlarr", "torbox", "jackett", "custom"]
    indexer_provider_labels = {
        "none": "None",
        "newznab": "Newznab",
        "nzbhydra": "NZBHydra",
        "torznab": "Torznab",
        "prowlarr": "Prowlarr",
        "torbox": "TorBox",
        "jackett": "Jackett",
        "custom": "Custom",
    }
    indexer_provider_defaults = {
        "newznab": "",
        "nzbhydra": "http://127.0.0.1:5076/?query={query_url}",
        "torznab": "",
        "prowlarr": "http://127.0.0.1:9696/",
        "torbox": "",
        "jackett": "http://127.0.0.1:9117/UI/Dashboard#search={query_url}",
        "custom": "",
    }
    old_jackett_default = "http://127.0.0.1:9117/UI/Dashboard?search={query_url}"

    if "config_indexer_provider_profiles" not in st.session_state:
        raw_profiles = config.get("indexer_provider_profiles", config.get("usenet_provider_profiles", {}))
        profiles = raw_profiles.copy() if isinstance(raw_profiles, dict) else {}
        legacy_provider = config.get(
            "indexer_provider",
            config.get("usenet_provider", config.get("secondary_missing_link_provider", "none")),
        )
        if legacy_provider in indexer_provider_options and legacy_provider != "none":
            legacy_template = config.get("indexer_web_url_template", config.get("usenet_web_url_template", ""))
            if legacy_provider == "jackett" and legacy_template == old_jackett_default:
                legacy_template = indexer_provider_defaults["jackett"]
            legacy_api_key = config.get(
                "indexer_api_key", config.get("usenet_api_key", config.get("secondary_missing_link_api_key", ""))
            )
            profiles.setdefault(
                legacy_provider,
                {
                    "indexer_web_url_template": str(legacy_template or ""),
                    "indexer_api_key": str(legacy_api_key or ""),
                },
            )
        jackett_profile = profiles.get("jackett")
        if isinstance(jackett_profile, dict):
            if jackett_profile.get("indexer_web_url_template") == old_jackett_default:
                jackett_profile["indexer_web_url_template"] = indexer_provider_defaults["jackett"]
                profiles["jackett"] = jackett_profile
        st.session_state["config_indexer_provider_profiles"] = profiles

    current_provider = config.get(
        "indexer_provider",
        config.get("usenet_provider", config.get("secondary_missing_link_provider", "none")),
    )
    if current_provider not in indexer_provider_options:
        current_provider = "none"
    if "config_selected_indexer_provider" not in st.session_state:
        st.session_state["config_selected_indexer_provider"] = current_provider
    if "config_prev_indexer_provider" not in st.session_state:
        st.session_state["config_prev_indexer_provider"] = st.session_state["config_selected_indexer_provider"]
    if "config_indexer_web_url_template" not in st.session_state:
        initial_profile = st.session_state["config_indexer_provider_profiles"].get(
            st.session_state["config_selected_indexer_provider"], {}
        )
        st.session_state["config_indexer_web_url_template"] = (
            initial_profile.get("indexer_web_url_template")
            or indexer_provider_defaults.get(st.session_state["config_selected_indexer_provider"], "")
        )
    if "config_indexer_api_key" not in st.session_state:
        initial_profile = st.session_state["config_indexer_provider_profiles"].get(
            st.session_state["config_selected_indexer_provider"], {}
        )
        st.session_state["config_indexer_api_key"] = initial_profile.get("indexer_api_key", "")

    plex_base_url = st.text_input("Plex Base URL", value=config.get("plex_base_url", ""))
    plex_token = st.text_input("Plex Token", value=config.get("plex_token", ""), type="password")
    tmdb_api_key = st.text_input("TMDB API Key", value=config.get("tmdb_api_key", ""), type="password")
    library_name = st.text_input("Library Name", value=config.get("library_name", ""))
    scan_scope = st.selectbox(
        "Scan Scope",
        options=["all_library", "watchlist_only"],
        index=0 if config.get("scan_scope", "all_library") != "watchlist_only" else 1,
        format_func=lambda v: "All Library Shows" if v == "all_library" else "Only Watchlisted TV Shows",
        help="Watchlist mode only scans TV shows that are present in your Plex watchlist. Movies are excluded.",
    )
    include_dmm_link = st.checkbox(
        "Enable DMM Link",
        value=bool(config.get("include_dmm_link", True)),
        help="Disable this if you only want provider links for missing episodes.",
    )
    dmm_base_url = st.text_input(
        "DMM Base URL",
        value=config.get("dmm_base_url", "https://debridmediamanager.com"),
        disabled=not include_dmm_link,
    )
    indexer_provider = st.selectbox(
        "Indexer Link Provider",
        options=indexer_provider_options,
        index=indexer_provider_options.index(st.session_state["config_selected_indexer_provider"]),
        format_func=lambda v: indexer_provider_labels.get(v, v),
    )

    if indexer_provider != st.session_state["config_prev_indexer_provider"]:
        previous_provider = st.session_state["config_prev_indexer_provider"]
        if previous_provider != "none":
            st.session_state["config_indexer_provider_profiles"][previous_provider] = {
                "indexer_web_url_template": st.session_state.get("config_indexer_web_url_template", ""),
                "indexer_api_key": st.session_state.get("config_indexer_api_key", ""),
            }
        selected_profile = st.session_state["config_indexer_provider_profiles"].get(indexer_provider, {})
        st.session_state["config_indexer_web_url_template"] = (
            selected_profile.get("indexer_web_url_template")
            or indexer_provider_defaults.get(indexer_provider, "")
        )
        st.session_state["config_indexer_api_key"] = selected_profile.get("indexer_api_key", "")
        st.session_state["config_prev_indexer_provider"] = indexer_provider
        st.session_state["config_selected_indexer_provider"] = indexer_provider
        trigger_rerun()

    indexer_web_url_template = st.text_input(
        "Provider Web URL Template",
        key="config_indexer_web_url_template",
        help=(
            "Example: http://127.0.0.1:5076/search?query={query_url}. "
            "Supported vars: {query}, {query_url}, {title}, {title_url}, {code}, {code_url}, "
            "{season}, {episode}, {imdbid}, {apikey}."
        ),
        disabled=(indexer_provider == "none"),
    )
    indexer_api_key = st.text_input(
        "Provider API Key (Optional)",
        key="config_indexer_api_key",
        type="password",
        help="Used when your URL template includes {apikey}.",
        disabled=(indexer_provider == "none"),
    )
    submitted = st.button("Save Configuration", key="save_config_btn")

    if submitted:
        if indexer_provider != "none":
            st.session_state["config_indexer_provider_profiles"][indexer_provider] = {
                "indexer_web_url_template": indexer_web_url_template.strip(),
                "indexer_api_key": indexer_api_key.strip(),
            }
        new_config = {
            "plex_base_url": plex_base_url.strip(),
            "plex_token": plex_token.strip(),
            "tmdb_api_key": tmdb_api_key.strip(),
            "library_name": library_name.strip(),
            "scan_scope": scan_scope,
            "include_dmm_link": bool(include_dmm_link),
            "dmm_base_url": dmm_base_url.strip(),
            "indexer_provider": indexer_provider,
            "indexer_api_key": indexer_api_key.strip(),
            "indexer_web_url_template": indexer_web_url_template.strip(),
            "indexer_provider_profiles": st.session_state["config_indexer_provider_profiles"],
        }
        save_config(new_config)
        st.session_state["app_config"] = new_config
        st.success("Configuration saved to config.json")
        trigger_rerun()

    st.markdown("---")
    st.subheader("App Update")
    st.caption("Checks git origin for newer commits and optionally performs a fast-forward update.")

    if "update_status" not in st.session_state:
        st.session_state["update_status"] = None
    if "update_feedback" not in st.session_state:
        st.session_state["update_feedback"] = None

    upd_col1, upd_col2 = st.columns([2, 2])
    with upd_col1:
        if st.button("Check for updates", key="check_updates_btn", width="stretch"):
            st.session_state["update_status"] = check_app_updates(fetch_remote=True)
            st.session_state["update_feedback"] = None
    with upd_col2:
        current_status = st.session_state.get("update_status") or {}
        can_update = (
            bool(current_status.get("ok"))
            and int(current_status.get("behind", 0) or 0) > 0
            and int(current_status.get("ahead", 0) or 0) == 0
        )
        if st.button("Update now", key="update_now_btn", disabled=not can_update, width="stretch"):
            ok, msg, status = update_app_from_remote()
            st.session_state["update_status"] = status
            if ok:
                st.session_state["update_feedback"] = {"level": "success", "text": msg}
            else:
                st.session_state["update_feedback"] = {"level": "error", "text": msg}

    status = st.session_state.get("update_status")
    feedback = st.session_state.get("update_feedback")
    if feedback:
        if feedback.get("level") == "success":
            st.success(feedback.get("text", ""))
        elif feedback.get("level") == "error":
            st.error(feedback.get("text", ""))
            if status and status.get("error"):
                st.caption(f"Details: {status['error']}")
        else:
            st.info(feedback.get("text", ""))

    if status:
        if status.get("ok"):
            st.info(status.get("message", ""))
            st.caption(
                f"Branch: {status.get('branch', '?')} | Behind: {status.get('behind', 0)} | "
                f"Ahead: {status.get('ahead', 0)}"
            )
            remote_url = status.get("remote_url")
            if remote_url:
                st.caption(f"Remote: {remote_url}")
        else:
            st.error(status.get("message", "Failed to check for updates."))
            if status.get("error"):
                st.caption(f"Details: {status['error']}")


def format_eta(air_date_str):
    try:
        air_date = datetime.strptime(air_date_str, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return "unknown", 10_000

    delta = (air_date - datetime.now().date()).days
    if delta < 0:
        return f"{abs(delta)} day(s) ago", delta
    if delta == 0:
        return "today", delta
    if delta == 1:
        return "tomorrow", delta
    if delta <= 7:
        return f"in {delta} days", delta
    return f"in ~{delta // 7} week(s)", delta


def render_upcoming_section(data):
    st.subheader("Upcoming Episodes")
    upcoming_items = [x for x in data if x.get("next_air")]
    if not upcoming_items:
        st.info("No upcoming episode date found.")
        return

    enriched = []
    for item in upcoming_items:
        eta_label, delta_days = format_eta(item["next_air"]["date"])
        enriched.append((delta_days, eta_label, item))
    enriched.sort(key=lambda x: x[0])

    spotlight = enriched[:3]
    if spotlight:
        st.caption("Closest upcoming episodes")
        cols = st.columns(len(spotlight))
        for idx, (_delta_days, eta_label, item) in enumerate(spotlight):
            with cols[idx]:
                poster_html = ""
                if item.get("poster_url"):
                    poster_html = f"<img class='upcoming-poster' src='{item['poster_url']}' alt='poster' />"
                st.markdown(
                    (
                        "<div class='upcoming-card'>"
                        f"{poster_html}"
                        "<div class='show-card-meta'>"
                        f"<div class='upcoming-title'>{item['title']}</div>"
                        f"<div class='upcoming-line'>{item['next_air']['code']}</div>"
                        f"<div class='upcoming-line'>{item['next_air']['date']}</div>"
                        f"<div class='upcoming-eta'>{eta_label}</div>"
                        "</div>"
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )
        st.markdown("<div style='margin-bottom: 0.75rem;'></div>", unsafe_allow_html=True)

    st.caption("Full timeline")
    for delta_days, eta_label, item in enriched:
        st.markdown(
            (
                "<div class='timeline-row'>"
                f"<div class='timeline-left'>{item['title']} - {item['next_air']['code']} ({item['next_air']['date']})</div>"
                f"<div class='timeline-right'>{eta_label}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )


def render_result_item(item, config, show_poster=True):
    cache = load_cache()
    cache_entry = cache.get(item["cache_key"], {})
    ignored = cache_entry.get("ignored_missing", [])
    ignore_all = bool(cache_entry.get("ignore_all_missing"))
    ensure_scan_state()
    scan_running = st.session_state["scan_state"]["running"]

    def render_refresh_button():
        if st.button("Refresh", key=f"refresh_{item['cache_key']}", disabled=scan_running):
            ok, msg, _ = refresh_single_show(item["cache_key"], config, get_results_map())
            if ok:
                st.success(msg)
                trigger_rerun()
            else:
                st.error(msg)

    if show_poster:
        poster_col, text_col = st.columns([1, 6])
        with poster_col:
            if item.get("poster_url"):
                st.image(item["poster_url"], width=82)
        with text_col:
            st.markdown(f"### {item.get('title', 'Unknown')}")
            st.write(f"{item.get('status', 'Unknown')} | {len(item.get('missing', []))} missing episodes")
        st.markdown("---")
    else:
        status_col, action_col = st.columns([4, 1])
        with status_col:
            st.caption(f"{item.get('status', 'Unknown')} | {len(item.get('missing', []))} missing episodes")
        with action_col:
            render_refresh_button()

    if show_poster:
        _, refresh_col = st.columns([5, 1])
        with refresh_col:
            render_refresh_button()

    with st.expander("Show Settings", expanded=False):
        tmdb_default = cache_entry.get("manual_tmdb_id") or item.get("tmdb_id") or ""
        tmdb_col1, tmdb_col2, tmdb_col3 = st.columns([2, 1, 1])
        with tmdb_col1:
            new_tmdb = st.text_input(
                "TMDB Override",
                value=str(tmdb_default),
                key=f"inline_tmdb_input_{item['cache_key']}",
            )
        with tmdb_col2:
            if st.button("Save", key=f"inline_tmdb_save_{item['cache_key']}", width="stretch"):
                ok, msg = set_tmdb_override(item["cache_key"], new_tmdb, config, get_results_map())
                if ok:
                    st.success(msg)
                    trigger_rerun()
                else:
                    st.error(msg)
        with tmdb_col3:
            if st.button("Clear", key=f"inline_tmdb_clear_{item['cache_key']}", width="stretch"):
                ok, msg = set_tmdb_override(item["cache_key"], None, config, get_results_map())
                if ok:
                    st.success(msg)
                    trigger_rerun()
                else:
                    st.error(msg)

        if st.button(
            "Ignore All Missing For Show",
            key=f"ignore_all_{item['cache_key']}",
            disabled=ignore_all,
            width="stretch",
        ):
            set_show_missing_ignore(item["cache_key"], ignore=True)
            trigger_rerun()

    if ignore_all:
        st.info("All missing episodes for this show are currently ignored.")

    for missing_code in item.get("missing", []):
        row_col1, row_col2 = st.columns([3, 1])
        with row_col1:
            links = build_missing_episode_links(item, missing_code, config)
            if not links:
                st.write(missing_code)
            elif len(links) == 1:
                st.link_button(links[0]["label"], links[0]["url"])
            else:
                link_col1, link_col2 = st.columns(2)
                with link_col1:
                    st.link_button(links[0]["label"], links[0]["url"])
                with link_col2:
                    st.link_button(links[1]["label"], links[1]["url"])
        with row_col2:
            if st.button("Ignore", key=f"ignore_{item['cache_key']}_{missing_code}"):
                set_episode_ignore(item["cache_key"], missing_code, ignore=True)
                trigger_rerun()

    if ignored:
        st.caption("Ignored episodes")
        for code in ignored:
            un_col1, un_col2 = st.columns([3, 1])
            with un_col1:
                st.write(code)
            with un_col2:
                if st.button("Unignore", key=f"unignore_{item['cache_key']}_{code}"):
                    set_episode_ignore(item["cache_key"], code, ignore=False)
                    trigger_rerun()

    st.caption(
        f"tmdb: {item.get('tmdb_id', 'N/A')} ({item.get('tmdb_source', 'unknown')}) | "
        f"imdb: {item.get('imdb_id') or 'N/A'}"
    )


def render_ongoing_grid(ongoing_missing, config):
    cols_per_row = 2
    for idx in range(0, len(ongoing_missing), cols_per_row):
        row = ongoing_missing[idx : idx + cols_per_row]
        cols = st.columns(cols_per_row)
        for cidx in range(cols_per_row):
            with cols[cidx]:
                if cidx >= len(row):
                    continue
                item = row[cidx]
                poster_html = ""
                if item.get("poster_url"):
                    poster_html = f"<img class='show-card-poster' src='{item['poster_url']}' alt='poster' />"
                st.markdown(
                    (
                        "<div class='show-grid-card'>"
                        f"{poster_html}"
                        "<div class='show-card-meta'>"
                        f"<div class='upcoming-title'>{item['title']}</div>"
                        f"<div class='upcoming-line'>{item.get('status', 'Unknown')}</div>"
                        f"<div class='upcoming-line'>{len(item.get('missing', []))} missing episodes</div>"
                        "</div>"
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )
                with st.expander("Details", expanded=False):
                    render_result_item(item, config, show_poster=False)


def render_ignored_management():
    st.subheader("Ignored Items Recovery")
    cache = load_cache()
    entries = sorted(
        [(k, v) for k, v in cache.items() if isinstance(v, dict)],
        key=lambda x: (x[1].get("title") or "").lower(),
    )

    ignored_shows = [(k, v) for k, v in entries if v.get("ignore_all_missing")]
    ignored_eps = [(k, v) for k, v in entries if v.get("ignored_missing")]

    st.caption(f"Ignored shows: {len(ignored_shows)} | Shows with ignored episodes: {len(ignored_eps)}")

    if ignored_shows:
        st.markdown("**Unignore Whole Shows**")
        for cache_key, entry in ignored_shows:
            c1, c2 = st.columns([4, 1])
            with c1:
                st.write(f"{entry.get('title', 'Unknown')} ({entry.get('year', 'N/A')})")
            with c2:
                if st.button("Unignore", key=f"settings_unignore_show_{cache_key}", width="stretch"):
                    set_show_missing_ignore(cache_key, ignore=False)
                    trigger_rerun()
    else:
        st.info("No shows are currently hidden via Ignore All.")

    if ignored_eps:
        st.markdown("**Unignore Episodes**")
        for cache_key, entry in ignored_eps:
            title = entry.get("title", "Unknown")
            year = entry.get("year", "N/A")
            with st.expander(f"{title} ({year}) - {len(entry.get('ignored_missing', []))} ignored episodes"):
                for code in entry.get("ignored_missing", []):
                    c1, c2 = st.columns([4, 1])
                    with c1:
                        st.write(code)
                    with c2:
                        if st.button("Unignore", key=f"settings_unignore_ep_{cache_key}_{code}", width="stretch"):
                            set_episode_ignore(cache_key, code, ignore=False)
                            trigger_rerun()
    else:
        st.info("No ignored episodes found.")


def render_results(config):
    ensure_scan_state()
    state = st.session_state["scan_state"]
    if not state["results_map"]:
        st.info("No scan results yet.")
        return

    data = apply_overrides_to_results(list(state["results_map"].values()))
    render_dashboard_summary(data)
    st.markdown("---")

    st.subheader("Missing Episodes - Ongoing Shows")
    ongoing_missing = [
        x for x in data if x.get("missing") and x.get("status") not in ENDED_STATUSES
    ]
    if not ongoing_missing:
        st.info("No missing episodes for ongoing shows.")
    else:
        sorted_ongoing = sorted(ongoing_missing, key=lambda x: x["title"].lower())
        render_ongoing_grid(sorted_ongoing, config)

    st.subheader("Missing Episodes - Ended/Canceled (Archive)")
    archived_missing = [x for x in data if x.get("missing") and x.get("status") in ENDED_STATUSES]
    if not archived_missing:
        st.info("No missing episodes in archived shows.")
    for item in sorted(archived_missing, key=lambda x: x["title"].lower()):
        with st.expander(f"{item['title']} ({item.get('status', 'Unknown')})"):
            render_result_item(item, config)

    render_upcoming_section(data)


def run_app():
    st.set_page_config(page_title="Plex Parity", layout="wide")
    apply_global_style()
    st.title("Plex Parity")
    st.caption("Track library freshness, fix mappings quickly, and prioritize what airs next.")

    if "app_config" not in st.session_state:
        st.session_state["app_config"] = load_config()
    app_config = st.session_state["app_config"]

    missing_config = validate_config(app_config)
    if missing_config:
        st.warning(
            "First-time setup required. Open `Settings` and save your Plex/TMDB values before scanning."
        )
        st.caption("Missing fields: " + ", ".join(missing_config))

    dashboard_tab, maintenance_tab, settings_tab = st.tabs(["Dashboard", "Maintenance", "Settings"])

    with dashboard_tab:
        render_scan_controls(app_config)
        st.divider()
        render_results(app_config)

    with maintenance_tab:
        render_tmdb_mapping_maintenance(app_config)
        st.markdown("---")
        render_ignored_management()

    with settings_tab:
        render_config_editor(app_config)
