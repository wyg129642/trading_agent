"""Industry Packs — per-industry configuration, recipes, and playbook.

Each subpackage is a self-contained "pack" that the system discovers at
runtime. Adding a new industry means dropping a new directory here — no
backend code changes required. See ``base_pack.py`` for the schema a
pack directory must follow.
"""
from .base_pack import IndustryPack, PackRegistry, pack_registry  # noqa: F401
