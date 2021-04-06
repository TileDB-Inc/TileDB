import sys
import hashlib


def print_and_die(message):
    print(message)
    sys.exit(1)


def parse_history(pr_message):
    type_tag = "TYPE:"
    description_tag = "DESC:"

    h_types = []
    h_descriptions = []

    lines = pr_message.split("\n")
    for line in lines:
        line = line.strip()
        if line.startswith(type_tag):
            h_types.append(line[len(type_tag) :].strip())
        if line.startswith(description_tag):
            h_descriptions.append(line[len(description_tag) :].strip())

    if len(h_types) == 0:
        print_and_die(
            'Unable to locate "' + type_tag + '" in PR message:\n' + pr_message
        )

    if len(h_descriptions) == 0:
        print_and_die(
            'Unable to locate "' + description_tag + '" in PR message:\n' + pr_message
        )

    if len(h_types) != len(h_descriptions):
        print_and_die("Mismatched number of history types and descriptions")

    return h_types, h_descriptions


def parse_type(h_type):
    if h_type == "FORMAT":
        return "## Disk Format"

    if h_type == "BREAKING_API":
        return "## Breaking C API changes"

    if h_type == "BREAKING_BEHAVIOR":
        return "## Breaking behavior"

    if h_type == "FEATURE":
        return "## New features"

    if h_type == "IMPROVEMENT":
        return "## Improvements"

    if h_type == "DEPRECATION":
        return "## Deprecations"

    if h_type == "BUG":
        return "## Bug fixes"

    if h_type == "C_API":
        return "### C API"

    if h_type == "CPP_API":
        return "### C++ API"

    print_and_die("Unknown history type: " + h_type)


# Returns a formatted line to insert into HISTORY.md
def format_history_line(pr_number, h_description):
    return (
        "* "
        + h_description
        + " [#"
        + pr_number
        + "](https://github.com/TileDB-Inc/TileDB/pull/"
        + pr_number
        + ")"
    )


# Updates the HISTORY.md file.
def update_history(pr_number, h_types, h_descriptions):
    # Read `HISTORY.md` into memory.
    lines = ""
    with open("HISTORY.md") as f:
        lines = f.read().rstrip("\n").split("\n")

    # Take the MD5 of `lines` so that we can determine
    # if this routine has modified `HISTORY.md`.
    orig_md5 = hashlib.md5()
    for line in lines:
        orig_md5.update(str.encode(line))

    # Remove all existing lines for this pull request.
    existing_line_substr = "https://github.com/TileDB-Inc/TileDB/pull/" + pr_number
    lines = [line for line in lines if not existing_line_substr in line]

    for i, h_type in enumerate(h_types):
        h_description = h_descriptions[i]

        history_header = parse_type(h_type)
        history_line = format_history_line(pr_number, h_description)

        # The HISTORY.md always starts with in-progress changes first.
        # We will locate the first line that starts with `history_header`
        for j in range(len(lines)):
            if lines[j].strip().startswith(history_header):
                lines = lines[0 : j + 1] + [history_line] + lines[j + 1 :]
                break

    # Take the new MD5 of `lines` so that we can determine
    # if this routine has modified `HISTORY.md`.
    new_md5 = hashlib.md5()
    for line in lines:
        new_md5.update(str.encode(line))

    if orig_md5.digest() == new_md5.digest():
        return False

    # Overwrite `HISTORY.md` with the new lines
    with open("HISTORY.md", "w") as f:
        for line in lines:
            f.write(line + "\n")

    return True


if len(sys.argv) < 3:
    print_and_die("usage: " + sys.argv[0] + " <PR number> <PR message>")

pr_number = sys.argv[1].strip()
pr_message = sys.argv[2].strip()

h_updated = False
if "NO_HISTORY" in pr_message:
    # If the user has included the reserved keyword "NO_HISTORY" anywhere in the
    # PR description, do not modify the history. Leave `h_updated` as `False`.
    pass
else:
    h_types, h_descriptions = parse_history(pr_message)
    h_updated = update_history(pr_number, h_types, h_descriptions)

# Set the Github Action variable `HISTORY_UPDATED`
print("::set-output name=HISTORY_UPDATED::" + str(h_updated))
