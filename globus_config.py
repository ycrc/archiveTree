#!/usr/bin/env python3
"""
Config file / env var / CLI precedence resolution shared by
archive_to_globus.py and restore_from_globus.py.

Precedence for every value: CLI flag > env var > config file > hard error
naming exactly which value is missing and all three ways to supply it.
"""

import configparser
import os
import sys

DEFAULT_CONFIG_FILE = os.path.expanduser("~/.archive_globus.cfg")


def load_config_file(path):
    """Return the [globus] section of an INI config file as a dict (possibly empty)."""
    if not path or not os.path.isfile(path):
        return {}
    # inline_comment_prefixes lets "key = value # comment" work; safe here
    # since none of our values (UUIDs, filesystem paths) legitimately
    # contain '#'.
    parser = configparser.ConfigParser(inline_comment_prefixes=("#",))
    parser.read(path)
    if "globus" not in parser:
        return {}
    return dict(parser["globus"])


def resolve(cli_value, env_var, config, config_key):
    """
    Return cli_value, or the env var, or the config file value, in that
    order, with `~` expanded (a no-op for values that aren't paths).
    """
    value = cli_value
    if value is None:
        value = os.environ.get(env_var) or None
    if value is None:
        value = config.get(config_key) or None
    if value is None:
        return None
    return os.path.expanduser(value)


def require(value, description, cli_flag, env_var, config_key):
    """Exit with a clear error if value is missing; otherwise return it unchanged."""
    if not value:
        print(
            f"ERROR: missing required value for {description}. Supply it via "
            f"the {cli_flag} flag, the {env_var} environment variable, or "
            f"'{config_key}' in the config file.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value
