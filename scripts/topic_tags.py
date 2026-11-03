"""Apply Terraform's desired tag colors through the Stream Catalog API."""

import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests

from scripts.terraform import get_project_root, run_terraform_output


def _request(session, method, url, **kwargs):
    """Retry catalog propagation and transient failures without logging credentials."""
    for attempt in range(5):
        try:
            response = session.request(method, url, timeout=30, **kwargs)
        except requests.RequestException:
            if attempt == 4:
                raise RuntimeError("Catalog request failed after five attempts") from None
        else:
            if response.ok:
                return response.json()
            if response.status_code not in (404, 429, 500, 502, 503, 504) or attempt == 4:
                raise RuntimeError(f"Catalog request failed: HTTP {response.status_code}")
        time.sleep(2**attempt)


def update_tag_color(session, endpoint: str, name: str, color: str) -> bool:
    """Preserve the definition, change its color, and verify a separate read."""
    url = endpoint.rstrip("/") + "/catalog/v1/types/tagdefs"
    tag_url = url + "/" + quote(name, safe="")
    tag = _request(session, "GET", tag_url)
    if tag.get("color") == color:
        return False
    payload = {
        key: tag[key] for key in ("name", "description", "entityTypes", "attributeDefs", "superTypes") if key in tag
    }
    payload["color"] = color
    result = _request(session, "PUT", url, json=[payload])
    if not isinstance(result, list) or len(result) != 1 or result[0].get("error"):
        raise RuntimeError(f"Catalog rejected color update for {name}")
    for attempt in range(5):
        if _request(session, "GET", tag_url).get("color") == color:
            return True
        if attempt < 4:
            time.sleep(2**attempt)
    raise RuntimeError(f"Color verification failed for {name}: expected {color}")


def apply_tag_colors(root: Path) -> None:
    core = run_terraform_output(root / "terraform/core/terraform.tfstate")
    demo = run_terraform_output(root / "terraform/airline-demo/terraform.tfstate")
    colors = demo.get("topic_tag_colors")
    if not colors:
        raise RuntimeError("Missing topic_tag_colors output; deploy the updated tag configuration first")
    with requests.Session() as session:
        session.auth = (core["app_manager_schema_registry_api_key"], core["app_manager_schema_registry_api_secret"])
        for name, color in sorted(colors.items()):
            changed = update_tag_color(session, core["confluent_schema_registry_rest_endpoint"], name, color)
            print(f"✓ {name}: {color} ({'updated and verified' if changed else 'already matches'})")


def main() -> None:
    try:
        apply_tag_colors(get_project_root())
    except (RuntimeError, KeyError, FileNotFoundError) as error:
        print(f"Tag colors failed: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
