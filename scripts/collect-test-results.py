#!/usr/bin/env python3

import os
import xml.dom.minidom as md
import xml.dom.pulldom as pd
import xml.etree.ElementTree as ET

ROOT_DIR=os.path.abspath(os.path.join(__file__, "..", ".."))

def attrs_to_dict(node):
  ret = {}
  for k, v in node.attributes.items():
    ret[k] = v
  return ret

def et_to_xml(node):
  xml = ET.tostring(node)
  xml = md.parseString(xml).toprettyxml(indent="  ")
  lines = xml.splitlines()
  if "?xml" in lines[0]:
    lines = lines[1:]
  return os.linesep.join(lines)

class TestCase(object):
  def __init__(self, node):
    attrs = attrs_to_dict(node)
    self.name = attrs["name"]
    self.filename = os.path.relpath(attrs["filename"], ROOT_DIR)
    self.line = attrs["line"]
    self.failed = False
    self.duration = 0

    results = node.getElementsByTagName("OverallResult")
    for r in results:
      attrs = attrs_to_dict(r)
      if attrs.get("success") != "true":
        self.failed = True
      self.duration += float(attrs.get("durationInSeconds", "0.0"))

class TestSuite(object):
  def __init__(self, handle):
    self.name = "<UNKNOWN>"
    self.tests = []
    self.assertion_results = {}
    self.test_results = {}
    self.failed = False

    doc = pd.parse(handle)
    for event, node in doc:
      if event != pd.START_ELEMENT:
        continue
      if node.tagName == "Catch2TestRun":
        self.name = attrs_to_dict(node).get("name", "<UNKNOWN>")
      elif node.tagName == "TestCase":
        doc.expandNode(node)
        self.tests.append(TestCase(node))
      elif node.tagName == "OverallResults":
        self.assertion_results = attrs_to_dict(node)
      elif node.tagName == "OverallResultsCases":
        self.test_results = attrs_to_dict(node)

    failures = int(self.test_results["failures"])
    expected = int(self.test_results["expectedFailures"])
    if failures > expected:
      self.failed = True

def add_suite(table, suite):
  result = "\u2705"
  if suite.failed:
    result = "\u274c"

  total_time = 0.0
  for t in suite.tests:
    total_time += t.duration
  total_time = "{:.2f}".format(total_time)

  row = ET.SubElement(table, "tr")
  ET.SubElement(row, "td").text = result
  ET.SubElement(row, "td").text = suite.name
  ET.SubElement(row, "td").text = total_time
  ET.SubElement(row, "td").text = suite.test_results["successes"]
  ET.SubElement(row, "td").text = suite.test_results["failures"]
  ET.SubElement(row, "td").text = suite.test_results["expectedFailures"]


def make_suites_table(suites):
  table = ET.Element("table")
  header = ET.SubElement(table, "tr")
  ET.SubElement(header, "th").text = "Result"
  ET.SubElement(header, "th").text = "Name"
  ET.SubElement(header, "th").text = "Time (Secs)"
  ET.SubElement(header, "th").text = "Successes"
  ET.SubElement(header, "th").text = "Failures"
  ET.SubElement(header, "th").text = "Expected Failures"

  for suite in sorted(suites, key=lambda s: s.name):
    add_suite(table, suite)

  return et_to_xml(table)


def make_failed_list(suites):
  ul = ET.Element("ul")
  for s in suites:
    if not s.failed:
      continue
    for t in s.tests:
      if not t.failed:
        continue
      ET.SubElement(ul, "li").text = t.name

  return et_to_xml(ul)

def main():
  suites = []
  for dpath, dnames, fnames in os.walk(ROOT_DIR):
    for fname in fnames:
      if fname.endswith("-catch2.xml"):
        fname = os.path.join(dpath, fname)
        with open(fname) as handle:
          suites.append(TestSuite(handle))

  print("Test Results")
  print("---")
  print()
  print(make_suites_table(suites))

  exit_code = 0
  if any(s.failed for s in suites):
    exit_code = 1
    print("Failed Tests")
    print("---")
    print()
    print(list_errors(suites))

  exit(exit_code)

if __name__ == "__main__":
    main()
