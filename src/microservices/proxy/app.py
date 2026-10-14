import os
import random
import logging
from urllib.parse import urljoin, urlparse

from flask import Flask, request, Response, jsonify
import requests

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PORT = int(os.getenv("PORT", "8000"))

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081")

migrated_routes_raw = os.getenv("MIGRATED_ROUTES", "api/movies")
MIGRATED_ROUTES = [r.strip() for r in migrated_routes_raw.split(",") if r.strip()]

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", False)

if GRADUAL_MIGRATION:
    new_traffic_percent_raw = os.getenv("MOVIES_MIGRATION_PERCENT", "0")
else:
    new_traffic_percent_raw = "0"

try:
    NEW_SERVICE_TRAFFIC_PERCENT = max(0, min(100, int(new_traffic_percent_raw)))
except ValueError:
    logger.warning("NEW_SERVICE_TRAFFIC_PERCENT не является целым числом, ставим 0")
    NEW_SERVICE_TRAFFIC_PERCENT = 0

if not MONOLITH_URL or not MOVIES_SERVICE_URL:
    raise RuntimeError("UPSTREAM_LEGACY и UPSTREAM_NEW должны быть заданы")

FORBIDDEN_HEADERS = {
    "host",
    "connection",
    "proxy-authorization",
    "proxy-authenticate",
}


def get_forwarded_headers():
    headers = {}
    for key, value in request.headers:
        if key.lower() in FORBIDDEN_HEADERS:
            continue
        headers[key] = value
    return headers


def is_path_in_list(path, prefixes):
    for prefix in prefixes:
        if path.startswith(prefix):
            return True
    return False


def choose_upstream(path):
    if not is_path_in_list(path, MIGRATED_ROUTES):
        return MONOLITH_URL, path

    if NEW_SERVICE_TRAFFIC_PERCENT == 100:
        return MOVIES_SERVICE_URL, path
    if NEW_SERVICE_TRAFFIC_PERCENT == 0:
        return MONOLITH_URL, path

    r = random.randint(0, 99)
    if r < NEW_SERVICE_TRAFFIC_PERCENT:
        return MOVIES_SERVICE_URL, path
    else:
        return MONOLITH_URL, path


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "service": "proxy"
    }), 200


@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"])
def proxy(path):
    upstream_base, target_path = choose_upstream(path)
    target_url = urljoin(upstream_base, target_path)

    parsed = urlparse(target_url)
    if parsed.scheme not in ("http", "https"):
        return Response("Invalid target URL scheme", status=400)

    logger.info("%s %s -> %s", request.method, request.path, target_url)

    try:
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=get_forwarded_headers(),
            data=request.get_data(),
            params=request.args,
            allow_redirects=False,
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        logger.error("Upstream request failed: %s", e)
        return Response("Upstream service unavailable", status=502)

    response = Response(resp.content, status=resp.status_code)
    for key, value in resp.headers.items():
        response.headers[key] = value

    cl = response.headers.get("Content-Length", "").lower().strip()
    if cl != "":
        response.headers.pop("Transfer-Encoding", None)

    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
