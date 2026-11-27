"""Django over OtterStax (examples/integration/CLAUDE.md, "Django"; docs/integration.md, section 8).

Two databases: "default" holds Django's own tables (migrations, auth, contenttypes) and the models
Django manages — OtterStax cannot create them (no identity / serial columns); "otterstax" reaches
every backend through one connection, for the models mapped onto backend tables (managed = False).
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SECRET_KEY = "example-only"
DEBUG = True
USE_TZ = True
TIME_ZONE = "UTC"

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "report",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "django_state.sqlite3",
    },
    "otterstax": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("OTX_DB", "local"),  # the session's place; the models name full tables
        "USER": "demo",
        "PASSWORD": "demo",
        "HOST": os.environ.get("OTX_HOST", "127.0.0.1"),
        "PORT": os.environ.get("OTX_PORT", "8817"),
    },
}
DATABASE_ROUTERS = ["report.routers.OtterStaxRouter"]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
