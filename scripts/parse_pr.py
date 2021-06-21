#!/usr/bin/env python3

import itertools as it
import sys
from operator import itemgetter
from typing import Mapping, Sequence


type_mapping = {
    "FORMAT": "## Disk Format",
    "BREAKING_API": "## Breaking C API changes",
    "BREAKING_BEHAVIOR": "## Breaking behavior",
    "FEATURE": "## New features",
    "IMPROVEMENT": "## Improvements",
    "DEPRECATION": "## Deprecations",
    "BUG": "## Bug fixes",
    "API": "## API additions",
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
                headers.append(type_mapping[change_type])
            except KeyError:
                raise ValueError(f"Unknown history type: {change_type}")

        elif line.startswith(description_tag):
            descriptions.append(line[len(description_tag) :].strip())

    if len(headers) != len(descriptions):
        raise ValueError("Mismatched number of history types and descriptions")

    get_header, get_description = itemgetter(0), itemgetter(1)
    header_descriptions = sorted(zip(headers, descriptions), key=get_header)
    return {
        header: list(map(get_description, group))
        for header, group in it.groupby(header_descriptions, key=get_header)
    }


def main():
    if len(sys.argv) > 1:
        body = sys.argv[1]
    else:
        body = sys.stdin.read()

    # If the reserved keyword "NO_HISTORY" is included anywhere
    # in the PR body, do not check it for validity
    if "NO_HISTORY" in body:
        return

    header_descriptions = parse_pr_body(body)
    if not header_descriptions:
        raise ValueError(f"Unable to locate history annotation in PR body")

    print("HISTORY.md to be updated with the following additions:")
    for header, descriptions in header_descriptions.items():
        print(f"\n{header}")
        for line in descriptions:
            print(line)


if __name__ == "__main__":
    try:
        main()
    except ValueError as ex:
        sys.exit(ex)
