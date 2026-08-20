#!/usr/bin/env python3
"""
Config file / env var / CLI precedence resolution for archive_to_globus.py
and restore_from_globus.py.

Thin wrapper around archive_config.py, scoped to the [globus] section of
the shared config file, kept for backward compatibility so existing
callers/config files need no changes.

Precedence for every value: CLI flag > env var > config file > hard error
naming exactly which value is missing and all three ways to supply it.
"""

import archive_config
from archive_config import DEFAULT_CONFIG_FILE, resolve, require  # noqa: F401


def load_config_file(path):
    """Return the [globus] section of an INI config file as a dict (possibly empty)."""
    return archive_config.load_config_file(path, section="globus")
