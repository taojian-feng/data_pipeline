"""Drafts the reliability agent proposes, and the human approval step that acts on them.

The agent never changes anything. It writes a *draft*: a GitHub issue, or a fix as a unified
diff against the repo's contracts or seed files. A person reviews it with
``gridlake incidents show`` and then approves or rejects it. Approval is the only path to an
effect: an approved issue is filed on GitHub (when ``GITHUB_TOKEN`` is set, else exported as
Markdown for ``gh issue create``), and an approved fix is written as a patch and, with
``--apply``, applied to the working tree for the normal pull-request review.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import requests

from gridlake.reliability.runlog import now_iso

GITHUB_API = "https://api.github.com"


@dataclass
class Draft:
    id: str
    incident_id: str
    kind: str  # issue | fix
    title: str
    body: str
    labels: list[str] = field(default_factory=list)
    path: str | None = None  # fix: the file the patch changes
    patch: str | None = None  # fix: unified diff, repo-relative paths
    status: str = "pending"  # pending | approved | rejected
    created: str = field(default_factory=now_iso)
    decided_by: str | None = None
    decided_at: str | None = None
    outcome: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Draft:
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})

    def render(self) -> str:
        head = f"[{self.status}] {self.id} ({self.kind}) for {self.incident_id}\n{self.title}\n"
        if self.labels:
            head += f"labels: {', '.join(self.labels)}\n"
        out = head + "\n" + self.body.strip() + "\n"
        if self.patch:
            out += "\n" + self.patch
        if self.outcome:
            out += f"\n{self.status} by {self.decided_by} at {self.decided_at}: {self.outcome}\n"
        return out


class DraftStore:
    """JSON drafts in a local directory, or under an S3 prefix on AWS."""

    def __init__(self, directory: Path, s3_uri: str | None = None, client: Any = None):
        self.directory, self.s3_uri = directory, s3_uri
        if s3_uri:
            if client is None:
                import boto3  # optional dependency, only needed on AWS

                client = boto3.client("s3")
            self.client = client
            self.bucket, _, prefix = s3_uri.removeprefix("s3://").partition("/")
            self.prefix = prefix.rstrip("/") + "/" if prefix else ""

    def save(self, draft: Draft) -> Draft:
        body = json.dumps(draft.to_dict(), indent=2)
        if self.s3_uri:
            self.client.put_object(
                Bucket=self.bucket, Key=f"{self.prefix}{draft.id}.json", Body=body.encode()
            )
        else:
            self.directory.mkdir(parents=True, exist_ok=True)
            (self.directory / f"{draft.id}.json").write_text(body + "\n")
        return draft

    def get(self, draft_id: str) -> Draft:
        if self.s3_uri:
            obj = self.client.get_object(Bucket=self.bucket, Key=f"{self.prefix}{draft_id}.json")
            return Draft.from_dict(json.loads(obj["Body"].read()))
        path = self.directory / f"{draft_id}.json"
        if not path.exists():
            raise LookupError(f"no draft {draft_id!r} in {self.directory}")
        return Draft.from_dict(json.loads(path.read_text()))

    def list(self, status: str | None = None) -> list[Draft]:
        if self.s3_uri:
            pages = self.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=self.prefix
            )
            ids = [
                o["Key"][len(self.prefix) : -5]
                for p in pages
                for o in p.get("Contents", [])
                if o["Key"].endswith(".json")
            ]
            drafts = [self.get(i) for i in ids]
        else:
            drafts = [
                Draft.from_dict(json.loads(p.read_text()))
                for p in sorted(self.directory.glob("*.json"))
            ]
        return sorted(
            (d for d in drafts if status is None or d.status == status), key=lambda d: d.created
        )

    def next_id(self, incident_id: str) -> str:
        taken = {d.id for d in self.list()}
        n = 1
        while f"{incident_id}-d{n}" in taken:
            n += 1
        return f"{incident_id}-d{n}"


class GitHubIssues:
    """Files an approved issue through the GitHub REST API."""

    def __init__(self, repo: str, token: str, session: requests.Session | None = None):
        self.repo, self.token = repo, token
        self.session = session or requests.Session()

    def create(self, title: str, body: str, labels: list[str]) -> str:
        resp = self.session.post(
            f"{GITHUB_API}/repos/{self.repo}/issues",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"title": title, "body": body, "labels": labels},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["html_url"]


def approve(
    store: DraftStore,
    draft_id: str,
    approver: str,
    out_dir: Path,
    repo_root: Path,
    github: GitHubIssues | None = None,
    apply: bool = False,
) -> Draft:
    """A person approves a pending draft; only now does anything outside the store change."""
    draft = store.get(draft_id)
    if draft.status != "pending":
        raise ValueError(f"{draft_id} is already {draft.status}")
    out_dir.mkdir(parents=True, exist_ok=True)
    if draft.kind == "issue":
        if github is not None:
            draft.outcome = f"filed {github.create(draft.title, draft.body, draft.labels)}"
        else:
            md = out_dir / f"{draft.id}.md"
            md.write_text(draft.body.strip() + "\n")
            labels = "".join(f" --label {lbl}" for lbl in draft.labels)
            draft.outcome = (
                f'exported {md}; file it with: gh issue create --title "{draft.title}" '
                f"--body-file {md}{labels}"
            )
    else:
        patch = out_dir / f"{draft.id}.patch"
        patch.write_text(draft.patch or "")
        if apply:
            for args in (["--check"], []):
                done = subprocess.run(
                    ["git", "apply", *args, str(patch)],
                    cwd=repo_root,
                    capture_output=True,
                    text=True,
                )
                if done.returncode:
                    raise RuntimeError(f"git apply failed: {done.stderr.strip()}")
            draft.outcome = f"applied {patch} to {repo_root}; review and open a pull request"
        else:
            draft.outcome = f"wrote {patch}; apply with: git apply {patch}"
    draft.status, draft.decided_by, draft.decided_at = "approved", approver, now_iso()
    return store.save(draft)


def reject(store: DraftStore, draft_id: str, approver: str, reason: str) -> Draft:
    draft = store.get(draft_id)
    if draft.status != "pending":
        raise ValueError(f"{draft_id} is already {draft.status}")
    draft.status, draft.decided_by, draft.decided_at = "rejected", approver, now_iso()
    draft.outcome = reason or "rejected"
    return store.save(draft)
