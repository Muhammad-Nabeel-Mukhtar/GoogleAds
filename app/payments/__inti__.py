# app/payments/__init__.py

from flask import Blueprint

payments_bp = Blueprint("payments", __name__, url_prefix="/api")

from . import routes  # noqa: E402,F401
