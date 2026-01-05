"""
Flatten a single Data18 scene JSON into a Google Sheet row.
"""

from typing import Any, Dict, List, Set, Tuple
from helpers import (
    to_title_case,
    normalize_name,
    normalize_whitespace,
    convert_duration,
    make_hyperlink,
    apply_network_alias,
)


def flatten_scene_to_row(
    scene: Dict[str, Any],
    pornstar_name: str,
    male_performers: Set[str],
    trans_performers: Set[str],
    site_to_network_map: Dict[str, str],
    hyperlinks_enabled: bool,
    format_title,
) -> Tuple[List[Any], Dict[str, str]]:
    """
    Returns:
        (
            row_values: List[Any],
            performer_links: Dict[str, str]
        )
    """

    # =========================================================
    # Performers (Columns E & F)
    # =========================================================
    performers = scene.get("performers", []) or []
    males, females = [], []
    performer_links: Dict[str, str] = {}

    for performer in performers:
        name = (performer.get("name") or "").strip()
        if not name:
            continue

        count = (performer.get("scenes_count") or "1").strip()
        display = f"{name} {{{count}}}"
        norm = normalize_name(name)

        url = performer.get("pair_url")
        if url:
            performer_links[norm] = url

        if norm in male_performers:
            males.append(display)
        elif norm in trans_performers:
            females.append(f"{display} (Trans)")
        else:
            females.append(display)

    # =========================================================
    # Raw entity extraction
    # =========================================================
    group_name = scene.get("group", {}).get("name")
    group_url = scene.get("group", {}).get("pair_url")

    network_name = scene.get("network", {}).get("name")
    network_url = scene.get("network", {}).get("pair_url")

    studio_name = scene.get("studio", {}).get("name")
    studio_url = scene.get("studio", {}).get("pair_url")

    site_name = scene.get("site", {}).get("name") or scene.get("webserie", {}).get(
        "name"
    )
    site_url = scene.get("site", {}).get("pair_url") or scene.get("webserie", {}).get(
        "pair_url"
    )

    # =========================================================
    # Column G — Network / Group / Standalone Studio
    # =========================================================
    if group_name:
        network_cell = (
            make_hyperlink(
                group_url,
                to_title_case(apply_network_alias(group_name)),
                hyperlinks_enabled,
            )
            if group_url
            else to_title_case(apply_network_alias(group_name))
        )

    elif network_name:
        network_cell = (
            make_hyperlink(
                network_url,
                to_title_case(apply_network_alias(network_name)),
                hyperlinks_enabled,
            )
            if network_url
            else to_title_case(apply_network_alias(network_name))
        )

    else:
        mapped_network = site_to_network_map.get(
            normalize_name(studio_name or ""), ""
        ) or site_to_network_map.get(normalize_name(site_name or ""), "")

        if mapped_network and normalize_name(mapped_network) != normalize_name(
            studio_name or ""
        ):
            network_cell = to_title_case(apply_network_alias(mapped_network))
        elif studio_name:
            network_cell = (
                make_hyperlink(
                    studio_url,
                    to_title_case(apply_network_alias(studio_name)),
                    hyperlinks_enabled,
                )
                if studio_url
                else to_title_case(apply_network_alias(studio_name))
            )
        else:
            network_cell = ""

    # =========================================================
    # Column H — Studio / Site / Webserie
    # =========================================================
    if group_name and studio_name:
        site_cell = (
            make_hyperlink(
                studio_url,
                to_title_case(apply_network_alias(studio_name)),
                hyperlinks_enabled,
            )
            if studio_url
            else to_title_case(apply_network_alias(studio_name))
        )

    elif studio_name and normalize_name(studio_name) in site_to_network_map:
        site_cell = (
            make_hyperlink(
                studio_url,
                to_title_case(apply_network_alias(studio_name)),
                hyperlinks_enabled,
            )
            if studio_url
            else to_title_case(apply_network_alias(studio_name))
        )

    elif site_name:
        site_cell = (
            make_hyperlink(
                site_url,
                to_title_case(apply_network_alias(site_name)),
                hyperlinks_enabled,
            )
            if site_url
            else to_title_case(apply_network_alias(site_name))
        )
    else:
        site_cell = ""

    # =========================================================
    # Title & Duration
    # =========================================================
    title = format_title(scene.get("scene_title", "") or "")
    duration = convert_duration(scene.get("details", {}).get("duration"))

    # =========================================================
    # Final Row (A–X)
    # =========================================================
    row = [
        None,  # A: ID (template-driven)
        pornstar_name,  # B: Pornstar
        scene.get("scene_id", ""),  # C: Scene ID
        scene.get("date", ""),  # D: Release Date
        "\n".join(map(normalize_whitespace, males)),  # E: Male Partners
        "\n".join(map(normalize_whitespace, females)),  # F: Female Partners
        network_cell,  # G: Network/Studio
        site_cell,  # H: Site/Webserie
        title,  # I: Title
        None,  # J: Thumbnail Image (template-driven)
        "Yes" if scene.get("is_vr_video") else "No",  # K: Is VR Video?
        None,  # L: TeleSave (template-driven)
        None,  # M: TeleLink
        None,  # N: Quality? (template-driven)
        None,  # O: File Size
        duration,  # P: Duration
        None,  # Q: Thumbnail
        None,  # R: ScreenCaps
        None,  # S: Pics Set
        None,  # T: Video Link
        scene.get("details", {}).get("original_site_final_url", ""),  # U: Original URL
        scene.get("scene_url", ""),  # V: Data18/IAFD URL
        scene.get("trailer_url", ""),  # W: Data18 Trailer URL
        None,  # X: TeleLabel (template-driven)
    ]

    return row, performer_links
