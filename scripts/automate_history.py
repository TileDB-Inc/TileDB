import argparse
import itertools as it
import sys
from collections import defaultdict
from operator import itemgetter
from typing import Mapping, Sequence, Tuple


_type_mapping = {
    "FORMAT": "## Disk Format",
    "BREAKING_API": "## Breaking C API changes",
    "BREAKING_BEHAVIOR": "## Breaking behavior",
    "FEATURE": "## New features",
    "IMPROVEMENT": "## Improvements",
    "DEPRECATION": "## Deprecations",
    "BUG": "## Bug fixes",
    "C_API": "### C API",
    "CPP_API": "### C++ API",
}


def parse_pr_body(
    body: str, type_tag: str = "TYPE:", description_tag: str = "DESC:"
) -> Mapping[str, Sequence[str]]:
    headers = []
    descriptions = []
    for line in body.strip().split("\n"):
        line = line.strip()
        if line.startswith(type_tag):
            change_type = line[len(type_tag) :].strip()
            try:
                headers.append(_type_mapping[change_type])
            except KeyError:
                raise ValueError(f"Unknown history type: {change_type}")

        elif line.startswith(description_tag):
            descriptions.append(line[len(description_tag) :].strip())

    if not headers:
        raise ValueError(f'Unable to locate "{type_tag}" in PR body:\n{body}')

    if not descriptions:
        raise ValueError(f'Unable to locate "{description_tag}" in PR body:\n{body}')

    if len(headers) != len(descriptions):
        raise ValueError("Mismatched number of history types and descriptions")

    get_header, get_description = itemgetter(0), itemgetter(1)
    header_descriptions = sorted(zip(headers, descriptions), key=get_header)
    return {
        header: list(map(get_description, group))
        for header, group in it.groupby(header_descriptions, key=get_header)
    }


# Updates the HISTORY.md file.
def update_history(
    pr_number: int,
    header_descriptions: Mapping[str, Sequence[str]],
) -> Mapping[str, Sequence[str]]:
    # Read `HISTORY.md` into memory.
    with open("HISTORY.md") as f:
        lines = f.read().rstrip("\n").split("\n")

    # Remove all existing lines for this pull request
    pull_url = f"https://github.com/TileDB-Inc/TileDB/pull/{pr_number}"
    new_lines = [line for line in lines if pull_url not in line]

    additions = defaultdict(list)
    for header, descriptions in header_descriptions.items():
        # The HISTORY.md always starts with in-progress changes first.
        # We will locate the first line that starts with `header`
        for i, line in enumerate(new_lines):
            if line.strip().startswith(header):
                added_lines = [f"* {description} [#{pr_number}]({pull_url})"
                               for description in descriptions]
                new_lines[i + 1: i + 1] = added_lines
                additions[header].extend(added_lines)
                break

    if lines == new_lines:
        additions.clear()
    else:
        # Overwrite `HISTORY.md` with the new lines
        with open("HISTORY.md", "w") as f:
            for line in new_lines:
                print(line, file=f)

    return additions


def main():
    parser = argparse.ArgumentParser(
        description="Check PR description and (optionally) update HISTORY.md"
    )
    parser.add_argument("number", type=int, help="The PR number")
    parser.add_argument("body", help="The PR text description body")
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check if the PR body is valid without updating HISTORY.md",
    )
    args = parser.parse_args()

    # If the reserved keyword "NO_HISTORY" is included anywhere in the PR body,
    # do not check it for validity or modify the history
    if "NO_HISTORY" in args.body:
        return

    try:
        header_descriptions = parse_pr_body(args.body)
    except ValueError as ex:
        sys.exit(ex)

    if args.check_only:
        return

    additions = update_history(args.number, header_descriptions)
    if additions:
        print("Updated HISTORY.md with the following additions:")
        for header, lines in additions.items():
            print()
            print(header)
            for line in lines:
                print(line)


if __name__ == "__main__":
    main()
