import sys

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


def parse_pr_message(pr_message, type_tag="TYPE:", description_tag="DESC:"):
    h_types = []
    h_descriptions = []
    for line in pr_message.strip().split("\n"):
        line = line.strip()
        if line.startswith(type_tag):
            h_types.append(line[len(type_tag) :].strip())
        elif line.startswith(description_tag):
            h_descriptions.append(line[len(description_tag) :].strip())

    if not h_types:
        sys.exit(f'Unable to locate "{type_tag}" in PR message:\n{pr_message}')

    if not h_descriptions:
        sys.exit(f'Unable to locate "{description_tag}" in PR message:\n{pr_message}')

    if len(h_types) != len(h_descriptions):
        sys.exit("Mismatched number of history types and descriptions")

    return h_types, h_descriptions


# Updates the HISTORY.md file.
def update_history(pr_number, h_types, h_descriptions):
    # Read `HISTORY.md` into memory.
    with open("HISTORY.md") as f:
        lines = f.read().rstrip("\n").split("\n")

    # Remove all existing lines for this pull request
    pr_number = pr_number.strip()
    pull_url = f"https://github.com/TileDB-Inc/TileDB/pull/{pr_number}"
    new_lines = [line for line in lines if pull_url not in line]

    for h_type, h_description in zip(h_types, h_descriptions):
        try:
            history_header = _type_mapping[h_type]
        except KeyError:
            sys.exit(f"Unknown history type: {h_type}")

        # The HISTORY.md always starts with in-progress changes first.
        # We will locate the first line that starts with `history_header`
        for i, line in enumerate(new_lines):
            if line.strip().startswith(history_header):
                new_lines.insert(i + 1, f"* {h_description} [#{pr_number}]({pull_url})")
                break

    if lines == new_lines:
        return False

    # Overwrite `HISTORY.md` with the new lines
    with open("HISTORY.md", "w") as f:
        for line in new_lines:
            print(line, file=f)

    return True


def main():
    try:
        pr_number, pr_message = sys.argv[1:]
    except ValueError:
        sys.exit(f"usage: {sys.argv[0]} <PR number> <PR message>")

    if "NO_HISTORY" in pr_message:
        # If the user has included the reserved keyword "NO_HISTORY" anywhere in the
        # PR description, do not modify the history. Leave `h_updated` as `False`.
        h_updated = False
    else:
        h_updated = update_history(pr_number, *parse_pr_message(pr_message))

    # Set the Github Action variable `HISTORY_UPDATED`
    print(f"::set-output name=HISTORY_UPDATED::{h_updated}")


if __name__ == "__main__":
    main()
