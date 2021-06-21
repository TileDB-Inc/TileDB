#!/usr/bin/env python3

import argparse
import logging
import re
import sys
from subprocess import check_output
from typing import Any, Dict, List, Optional

from github import Github

from parse_pr import parse_pr_body, type_mapping


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare next TileDB release")
    parser.add_argument("version", help="New release version")
    parser.add_argument(
        "--head",
        help="Git commit (or branch/tag) of the head for the new release (default: current branch)",
    )
    parser.add_argument(
        "--base",
        help="Git commit (or branch/tag) of the base for the new release (default: latest release)",
    )
    parser.add_argument(
        "--token",
        help="GitHub OAuth token for accessing the GitHub API (default: unauthenticated access)",
    )
    args = parser.parse_args()

    version_re = r"^\d+\.\d+\.\d+$"
    if not re.match(version_re, args.version):
        sys.exit(f"Invalid version '{args.version}': must match '{version_re}'")

    prs = find_prs(Github(args.token), args.head, args.base)
    update_history(args.version, prs)

    update_version(args.version)


def find_prs(gh: Github, head: str, base: Optional[str] = None) -> Dict[int, str]:
    repo = gh.get_repo("TileDB-Inc/TileDB")

    if base is None:
        latest_release = repo.get_latest_release()
        base = latest_release.tag_name
        base_sha = latest_release.target_commitish
    else:
        base_sha = repo.get_commit(base).sha
    logging.info(f"Base: {base} ({base_sha})")

    if head is None:
        head = check_output(["git", "branch", "--show-current"]).strip().decode()
        head_sha = check_output(["git", "rev-parse", head]).strip().decode()
    else:
        head_sha = repo.get_commit(head).sha
    logging.info(f"Head: {head} ({head_sha})")

    comparison = repo.compare(base_sha, head_sha)
    if comparison.behind_by != 0:
        raise ValueError(f"Head is {comparison.behind_by} commits behind base")
    if comparison.ahead_by == 0:
        raise ValueError("Head is not ahead of base")
    logging.info(f"Head is {comparison.ahead_by} commits ahead of base")

    prs = {}
    for commit in comparison.commits:
        for pr in commit.get_pulls():
            backport = re.match(r"^backport-(\d+)", pr.head.ref)
            if backport:
                pr = repo.get_pull(int(backport.group(1)))
            # If the reserved keyword "NO_HISTORY" is included anywhere in the PR body,
            # ignore the PR
            if "NO_HISTORY" not in pr.body:
                prs[pr.number] = pr.body

    logging.info(
        f"{len(prs)} unique PRs with HISTORY notes found related to commits between "
        f"base and head: {sorted(prs)}"
    )
    return prs


def update_history(version: str, prs: Dict[int, str]) -> None:
    sections: Dict[str, List[str]] = {header: [] for header in type_mapping.values()}
    for pr_number, pr_body in prs.items():
        pull_url = f"https://github.com/TileDB-Inc/TileDB/pull/{pr_number}"
        for header, descriptions in parse_pr_body(pr_body).items():
            for description in descriptions:
                sections[header].append(f"* {description} [#{pr_number}]({pull_url})")

    # Read `HISTORY.md` into memory.
    with open("HISTORY.md") as f:
        current_history = f.read()

    # Overwrite `HISTORY.md` with the new lines
    with open("HISTORY.md", "w") as f:
        print(f"# TileDB v{version} Release Notes", file=f)
        for header, lines in sections.items():
            # ignore empty subsections
            if header.startswith("###") and not lines:
                continue
            print(file=f)
            print(header, file=f)
            for line in lines:
                print(line, file=f)

        # append the current history
        print(file=f)
        f.write(current_history)


def update_version(version: str) -> None:
    # 1. Replace "In Progress" in HISTORY.md
    replace_in_file(
        "HISTORY.md",
        dict(
            pattern=r"# In Progress\n",
            repl=f"# TileDB v{version} Release Notes\n",
        ),
    )

    # 2. replace major, minor, patch in tiledb/sm/c_api/tiledb_version.h
    major, minor, patch = version.split(".")
    replace_in_file(
        "tiledb/sm/c_api/tiledb_version.h",
        dict(
            pattern=r"#define TILEDB_VERSION_MAJOR \d+\n",
            repl=f"#define TILEDB_VERSION_MAJOR {major}\n",
        ),
        dict(
            pattern=r"#define TILEDB_VERSION_MINOR \d+\n",
            repl=f"#define TILEDB_VERSION_MINOR {minor}\n",
        ),
        dict(
            pattern=r"#define TILEDB_VERSION_PATCH \d+\n",
            repl=f"#define TILEDB_VERSION_PATCH {patch}\n",
        ),
    )

    # 3. replace version and release in doc/source/conf.py
    replace_in_file(
        "doc/source/conf.py",
        dict(
            pattern=r"version = '\d+\.\d+'\n",
            repl=f"version = '{major}.{minor}'\n",
        ),
        dict(
            pattern=r"release = '\d+\.\d+\.\d+'\n",
            repl=f"release = '{major}.{minor}.{patch}'\n",
        ),
    )


def replace_in_file(path: str, *replacement_kwargs: Dict[str, Any]) -> None:
    with open(path) as f:
        text = f.read()

    new_text = text
    for kwargs in replacement_kwargs:
        new_text = re.sub(string=new_text, **kwargs)

    if new_text != text:
        with open(path, "w") as f:
            f.write(new_text)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s]: %(message)s")
    main()
