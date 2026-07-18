from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

# 매니페스트 GET 시 제시할 Accept — 이미지 매니페스트와 멀티아치 인덱스 모두.
_MANIFEST_ACCEPT = ", ".join(
    [
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.index.v1+json",
    ]
)
_INDEX_TYPES = {
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.index.v1+json",
}


def _request(
    method: str, url: str, accept: str | None = None
) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(url, method=method)
    if accept:
        req.add_header("Accept", accept)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _get_json(url: str, accept: str | None = None) -> dict:
    status, _, body = _request("GET", url, accept)
    if status != 200:
        raise RuntimeError(f"GET {url} -> {status}")
    return json.loads(body)


def _created_at(base: str, repo: str, tag: str) -> str:
    status, headers, body = _request(
        "GET", f"{base}/v2/{repo}/manifests/{tag}", _MANIFEST_ACCEPT
    )
    if status != 200:
        return ""
    manifest = json.loads(body)
    if headers.get("Content-Type") in _INDEX_TYPES:
        children = manifest.get("manifests", [])
        if not children:
            return ""
        child_digest = children[0]["digest"]
        status, _, body = _request(
            "GET", f"{base}/v2/{repo}/manifests/{child_digest}", _MANIFEST_ACCEPT
        )
        if status != 200:
            return ""
        manifest = json.loads(body)
    config_digest = manifest.get("config", {}).get("digest")
    if not config_digest:
        return ""
    config = _get_json(f"{base}/v2/{repo}/blobs/{config_digest}")
    return config.get("created", "")


def _manifest_digest(base: str, repo: str, tag: str) -> str | None:
    status, headers, _ = _request(
        "GET", f"{base}/v2/{repo}/manifests/{tag}", _MANIFEST_ACCEPT
    )
    if status != 200:
        return None
    return headers.get("Docker-Content-Digest")


def prune_repo(base: str, repo: str, keep: int) -> int:
    data = _get_json(f"{base}/v2/{repo}/tags/list")
    tags = data.get("tags") or []
    if len(tags) <= keep:
        print(f"  {repo}: {len(tags)} tags <= keep={keep} - skip")
        return 0

    # 생성일 조회 실패("") 태그는 나이 불명이라 정렬 기준으로 삼을 수 없음 —
    # drop 후보에서 제외하고 무조건 keep (일시적 fetch 실패로 최신 태그가
    # 삭제되는 사고 방지).
    dated = [(tag, _created_at(base, repo, tag)) for tag in tags]
    undated_tags = [tag for tag, created in dated if not created]
    known_sorted = sorted(
        (tc for tc in dated if tc[1]), key=lambda tc: tc[1], reverse=True
    )
    keep_tags = undated_tags + [tag for tag, _ in known_sorted[:keep]]
    drop_tags = [tag for tag, _ in known_sorted[keep:]]
    print(f"  {repo}: {len(tags)} tags -> keep {len(keep_tags)}, drop {len(drop_tags)}")

    # drop 태그와 digest 를 공유하는 keep 태그가 있으면 DELETE 시 keep 태그도
    # 함께 깨짐 — keep 쪽 digest 를 먼저 확보해 그런 drop 은 skip.
    keep_digests = {
        digest
        for tag in keep_tags
        if (digest := _manifest_digest(base, repo, tag))
    }

    deleted: set[str] = set()
    count = 0
    for tag in drop_tags:
        digest = _manifest_digest(base, repo, tag)
        if not digest or digest in deleted:
            continue
        if digest in keep_digests:
            print(f"    skip {tag} ({digest[:19]}...) - shared with kept tag")
            continue
        deleted.add(digest)
        status, _, _ = _request("DELETE", f"{base}/v2/{repo}/manifests/{digest}")
        if status == 202:
            print(f"    deleted {tag} ({digest[:19]}...)")
            count += 1
        else:
            print(f"    FAILED {tag} -> {status}", file=sys.stderr)
    return count


def run(registry_url: str, keep: int = 5) -> int:
    base = registry_url.rstrip("/")
    repos = _get_json(f"{base}/v2/_catalog").get("repositories") or []
    print(f"registry {base}: {len(repos)} repos, keep={keep}")
    total = sum(prune_repo(base, repo, keep) for repo in repos)
    print(f"total {total} manifests deleted")
    return total
