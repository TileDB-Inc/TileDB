import argparse
import logging
import re
import sys
from subprocess import check_output
from typing import Any, Mapping, Optional

from github import Github

from parse_pr import parse_pr_body


def main():
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
    for pr in prs.items():
        update_history(*pr)

    update_version(args.version)


def find_prs(gh: Github, head: str, base: Optional[str] = None) -> Mapping[int, str]:
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
        raise ValueError(f"Head is not ahead of base")
    logging.info(f"Head is {comparison.ahead_by} commits ahead of base")

    prs = {
        pr.number: pr.body for commit in comparison.commits for pr in commit.get_pulls()
    }
    logging.info(
        f"{len(prs)} unique PRs found related to commits between base and head: {sorted(prs)}"
    )
    return prs


def update_history(pr_number: int, pr_body: str) -> None:
    header_descriptions = parse_pr_body(pr_body)

    # Read `HISTORY.md` into memory.
    with open("HISTORY.md") as f:
        lines = f.read().rstrip("\n").split("\n")

    # Remove all existing lines for this pull request
    pull_url = f"https://github.com/TileDB-Inc/TileDB/pull/{pr_number}"
    new_lines = [line for line in lines if pull_url not in line]
    for header, descriptions in header_descriptions.items():
        # The HISTORY.md always starts with in-progress changes first.
        # We will locate the first line that starts with `header`
        # and insert the descriptions
        for i, line in enumerate(new_lines):
            if line.strip().startswith(header):
                new_lines[i + 1 : i + 1] = [
                    f"* {description} [#{pr_number}]({pull_url})"
                    for description in descriptions
                ]
                break

    if lines != new_lines:
        # Overwrite `HISTORY.md` with the new lines
        with open("HISTORY.md", "w") as f:
            for line in new_lines:
                print(line, file=f)


def update_version(version):
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


def replace_in_file(path: str, *replacement_kwargs: Mapping[str, Any]) -> None:
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
