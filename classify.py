"""Classify AIS vessels into operational categories from type + size.

AIS `ship_type` is an integer 0–99 assigned per ITU-R M.1371.  The
categories captains care about (cruise, commercial, fishing) map
loosely onto the codes — but not precisely:

  * Type 60–69 covers ALL passenger vessels.  That includes cruise
    ships, but ALSO small passenger ferries and day-tour boats.  We
    only want to tag cruise ships as "cruise" because that's what
    drives the map treatment — so gate on length.  ~150 m is roughly
    the lower bound of an ocean-going cruise ship (Silversea / small
    expedition ships are 140–180 m; modern mainstream cruise ships
    are 250–350 m).
  * Types 70–89 are cargo + tanker — commercial either way, display
    treatment is identical.
  * Type 30 is "fishing".  We separate commercial fishing from
    recreational but AIS doesn't give us that — tag all as "fishing"
    and let the UI layer differentiate if it ever matters.
  * Everything else → "other".  Includes sailing vessels (36), tugs
    (52), pilot vessels (50), SAR (51), pleasure craft (37), and the
    many "no additional info" buckets.

This module is intentionally stateless — pass in the AIS fields and
get back a category string.  No DB, no I/O, easy to unit-test.
"""
from __future__ import annotations

from typing import Optional


# Length threshold above which a passenger vessel qualifies as
# "cruise" in the UI sense.  Value in metres.  Tune here if Geoff
# wants small-ship cruise lines (Lindblad, UnCruise) tagged as
# cruise — they run ~70–90 m boats.
CRUISE_MIN_LENGTH_M = 150


VesselKind = str  # Literal["cruise", "commercial", "fishing", "other"]


def classify_vessel(
    ship_type: Optional[int],
    length_m: Optional[int],
) -> VesselKind:
    """Return a kind tag given AIS ship_type and (optional) length.

    `length_m` may be None when we've only seen a position report and
    no static-data message yet.  In that case a passenger vessel is
    tentatively tagged "other" — it'll get re-classified the first
    time a static-data message populates length.
    """
    if ship_type is None:
        return "other"

    if 60 <= ship_type <= 69:
        # Passenger: cruise if sufficiently large, otherwise ferry /
        # day-tour / small charter.
        if length_m is not None and length_m >= CRUISE_MIN_LENGTH_M:
            return "cruise"
        return "other"

    if 70 <= ship_type <= 89:
        return "commercial"

    if ship_type == 30:
        return "fishing"

    return "other"
