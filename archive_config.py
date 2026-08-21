#!/usr/bin/env python3
"""
Config file / env var / CLI precedence resolution shared across archiveTree's
backend scripts (archive_to_s3.py, archive_to_globus.py, restore_from_s3.py,
restore_from_globus.py) and the unified archive.py / restore.py dispatchers.

Precedence for every value: CLI flag > env var (if any) > config file >
hard error naming exactly which values are missing and how to supply them.

The config file is a single INI file (default ~/.archive.cfg,
overridable via --config-file / GLOBUS_ARCHIVE_CONFIG) with one section per
concern, e.g. [archive] for backend selection, [s3] for S3 settings,
[globus] for Globus settings. globus_config.py is a thin backward-compatible
wrapper around this module, scoped to the [globus] section.
"""

import configparser
import os
import sys

DEFAULT_CONFIG_FILE = os.path.expanduser("~/.archive.cfg")


def load_config_file(path, section):
    """Return the given INI section of a config file as a dict (possibly empty)."""
    if not path or not os.path.isfile(path):
        return {}
    # inline_comment_prefixes lets "key = value # comment" work; safe here
    # since none of our values (UUIDs, filesystem paths) legitimately
    # contain '#'.
    parser = configparser.ConfigParser(inline_comment_prefixes=("#",))
    parser.read(path)
    if section not in parser:
        return {}
    return dict(parser[section])


def resolve(cli_value, env_var, config, config_key):
    """
    Return cli_value, or the env var (if env_var is given), or the config
    file value, in that order, with `~` expanded (a no-op for values that
    aren't paths). Pass env_var=None to skip the environment-variable step
    entirely (used by backends with no per-field env vars, e.g. S3).
    """
    value = cli_value
    if value is None and env_var:
        value = os.environ.get(env_var) or None
    if value is None:
        value = config.get(config_key) or None
    if value is None:
        return None
    return os.path.expanduser(value)


def require(value, description, cli_flag, env_var, config_key):
    """Exit with a clear error if value is missing; otherwise return it unchanged."""
    if not value:
        via = [f"the {cli_flag} flag"]
        if env_var:
            via.append(f"the {env_var} environment variable")
        via.append(f"'{config_key}' in the config file")
        print(
            f"ERROR: missing required value for {description}. Supply it via "
            + ", ".join(via[:-1]) + f", or {via[-1]}.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


def resolve_backend(cli_backend, archive_section, valid=("s3", "globus", "local")):
    """
    Determine which backend to use: CLI --backend > config [archive]
    'backend' key.

    Returns (backend, None) on success, (None, None) if nothing specified
    (caller decides how to report that), or (None, error_message) if an
    invalid value was given.
    """
    backend = cli_backend or (archive_section or {}).get("backend")
    if not backend:
        return None, None
    if backend not in valid:
        return None, f"ERROR: invalid backend {backend!r}; must be one of {', '.join(valid)}."
    return backend, None
