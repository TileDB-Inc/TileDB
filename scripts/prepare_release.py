#!/usr/bin/env python3

import argparse
import logging
import os
import pickle
import re
import sys
from subprocess import check_output
from typing import Any, Dict, List, Optional

from github import Github

from parse_pr import parse_pr_body, type_mapping

history_cache_file = "prepare-history-cache.pickle"
sphinx_conf_file = "tiledb/doxygen/source/conf.py"

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
    parser.add_argument(
        "--cache-history",
        default=False,
        type=bool,
        action=argparse.BooleanOptionalAction, # nb: requires Python 3.9+
        help="Cache the GitHub PR listing for script development iteration (default: False)",
    )

    args = parser.parse_args()

    version_re = r"^\d+\.\d+\.\d+$"
    if not re.match(version_re, args.version):
        sys.exit(f"Invalid version '{args.version}': must match '{version_re}'")

    if args.cache_history and os.path.isfile(history_cache_file):
      logging.warning("Using cached GitHub PR info with NO VALIDATION of branch/head/etc.")
      prs = pickle.load(open(history_cache_file, "rb"))
    else:
      prs = find_prs(Github(args.token), args.head, args.base)
      if args.cache_history:
        logging.info(f"Saving GitHub PR info to cache '{history_cache_file}")
        pickle.dump(prs, open(history_cache_file, "wb"))

    update_history(args.version, prs)

    update_version(args.version)

def parse_existing_prs(history: str):
    # Returns all previously listed PR numbers in HISTORY file
    match_groups = [r"\(#(=\d+)\)", r"\[\#(\d+)\]"]
    expr = re.compile("|".join(match_groups))
    # Flatten ('', '1234'). Only one group should ever match.
    existing_prs = [a+b for a,b in expr.findall(history)]
    return set(map(int, existing_prs))

def find_prs(gh: Github, head: str, base: Optional[str] = None) -> Dict[int, str]:
    repo = gh.get_repo("TileDB-Inc/TileDB")

    if base is None:

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
        find_common_ancestor = input(
            f"Head is {comparison.behind_by} commits behind base: do you want to use the "
            f"latest common ancestor of {base} and {head} as base? [y/n] "
        )
        if find_common_ancestor.lower() == "y":
            base_sha = comparison.merge_base_commit.sha
            comparison = repo.compare(base_sha, head_sha)
            assert comparison.behind_by == 0, comparison.behind_by
        else:
            sys.exit(f"Aborting: head is {comparison.behind_by} commits behind base")
    logging.info(f"Head is {comparison.ahead_by} commits ahead of base")

    prs = {}
    for commit in comparison.commits:
        for pr in commit.get_pulls():
            backport = re.match(r"^backport-(\d+)", pr.head.ref)
            if backport:
                pr = repo.get_pull(int(backport.group(1)))
            # If the pr is open or the reserved keyword "NO_HISTORY" is included anywhere
            # in the PR body, ignore the PR
            if pr.state != "open" and pr.body and "NO_HISTORY" not in pr.body:
                prs[pr.number] = pr.body

    return prs


def update_history(version: str, prs: Dict[int, str]) -> None:
    # Read `HISTORY.md` into memory.
    with open("HISTORY.md") as f:
        current_history = f.read()

    existing_prs = parse_existing_prs(current_history)

    sections: Dict[str, List[str]] = {header: [] for header in type_mapping.values()}
    skipped: List[int] = []
    for pr_number, pr_body in prs.items():
        pull_url = f"https://github.com/TileDB-Inc/TileDB/pull/{pr_number}"
        for header, descriptions in parse_pr_body(pr_number, pr_body).items():
            if pr_number in existing_prs:
                skipped.append(pr_number)
                continue

            for description in descriptions:
                sections[header].append(f"* {description} [#{pr_number}]({pull_url})")

    logging.info(
        f"{len(prs)} unique PRs with HISTORY notes found related to commits between "
        f"base and head: {sorted(prs)}"
    )
    logging.info(f"Skipping existing PRs: {sorted(skipped)}")

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
    logging.info(f"Updating 'tiledb_version.h' to {major}.{minor}.{patch}")
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
    logging.info(f"Updating 'sphinx conf.py' to {major}.{minor}.{patch}")
    replace_in_file(
        sphinx_conf_file,
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
