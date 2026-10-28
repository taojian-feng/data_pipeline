"""Approving a data product draft: the only step that changes the repository.

``approve`` without ``--pr`` behaves like the reliability agent's approval: it writes the patch
and, with ``apply``, applies it to a working tree. With ``--pr`` it commits the three files on
a new branch in a temporary git worktree (the person's checkout is never touched), pushes the
branch, and opens a pull request when ``GITHUB_TOKEN`` and ``GRIDLAKE_GITHUB_REPO`` are set,
else prints the ``gh pr create`` command. The pull request then gets the normal review, and CI
runs the drafted tests against the product.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import requests

from gridlake.reliability.drafts import GITHUB_API, Draft, DraftStore
from gridlake.reliability.drafts import approve as approve_patch
from gridlake.reliability.runlog import now_iso


class GitHubPulls:
    """Opens a pull request through the GitHub REST API."""

    def __init__(self, repo: str, token: str, session: requests.Session | None = None):
        self.repo, self.token = repo, token
        self.session = session or requests.Session()

    def create(self, title: str, body: str, head: str, base: str) -> str:
        resp = self.session.post(
            f"{GITHUB_API}/repos/{self.repo}/pulls",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"title": title, "body": body, "head": head, "base": base},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["html_url"]


def _git(cwd: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    if done.returncode:
        raise RuntimeError(f"git {' '.join(args[:2])} failed: {done.stderr.strip()}")
    return done.stdout.strip()


def branch_name(draft: Draft) -> str:
    return "data-product/" + draft.incident_id.removeprefix("dp-").replace("_", "-")


def open_pull_request(
    store: DraftStore,
    draft_id: str,
    approver: str,
    repo_root: Path,
    base: str = "main",
    remote: str = "origin",
    github: GitHubPulls | None = None,
    push: bool = True,
) -> Draft:
    """A person approves a pending product draft; commit it on a branch and open the PR."""
    draft = store.get(draft_id)
    if draft.status != "pending":
        raise ValueError(f"{draft_id} is already {draft.status}")
    if draft.kind != "product" or not draft.patch:
        raise ValueError(f"{draft_id} is not a data product draft")
    branch = branch_name(draft)
    with tempfile.TemporaryDirectory() as tmp:
        tree, patch = Path(tmp) / "tree", Path(tmp) / f"{draft.id}.patch"
        patch.write_text(draft.patch)
        _git(repo_root, "worktree", "add", "-b", branch, str(tree), base)
        try:
            _git(tree, "apply", "--index", str(patch))
            identity = []
            if not _git_config(tree, "user.email"):
                identity = [
                    "-c",
                    f"user.name={approver}",
                    "-c",
                    f"user.email={approver}@users.noreply.github.com",
                ]
            message = f"{draft.title}\n\nDrafted by the gridlake self-service data product agent ({draft.id}).\nApproved-by: {approver}"
            _git(tree, *identity, "commit", "-q", "-m", message)
            if push:
                _git(tree, "push", "-q", "-u", remote, branch)
        finally:
            _git(repo_root, "worktree", "remove", "--force", str(tree))
    target = base.split("/", 1)[1] if base.startswith(f"{remote}/") else base
    if not push:
        draft.outcome = (
            f"committed on branch {branch}; push it and open a pull request into {target}"
        )
    elif github is not None:
        draft.outcome = f"opened {github.create(draft.title, draft.body, branch, target)}"
    else:
        draft.outcome = (
            f"pushed {branch}; open the pull request with: gh pr create --base {target} "
            f'--head {branch} --title "{draft.title}" --body "$(gridlake products show {draft.id} --body)"'
        )
    draft.status, draft.decided_by, draft.decided_at = "approved", approver, now_iso()
    return store.save(draft)


def _git_config(cwd: Path, key: str) -> str:
    done = subprocess.run(["git", "config", key], cwd=cwd, capture_output=True, text=True)
    return done.stdout.strip()


def approve(
    store: DraftStore,
    draft_id: str,
    approver: str,
    out_dir: Path,
    repo_root: Path,
    apply: bool = False,
) -> Draft:
    """Approve without a pull request: write the patch, and apply it with ``apply``."""
    if store.get(draft_id).kind != "product":
        raise ValueError(f"{draft_id} is not a data product draft")
    return approve_patch(store, draft_id, approver, out_dir, repo_root, apply=apply)
