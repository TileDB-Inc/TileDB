---
title: Current Domain
---

## Main Structure

The array current domain is stored within the [Array Schema](./array_schema.md#array-schema-file)
file. If a current domain is empty, only the version number and the empty flag are serialized to storage.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | Current domain version number |
| Empty | `uint8_t` | Whether the current domain has a representation(e.g. NDRectangle) set or not |
| Type | `uint8_t` | The type of current domain stored in this file |
| NDRectangle | [MBR](./fragment.md#mbr) | A hyperrectangle defined using [1DRange](./fragment.md#mbr) items for each dimension |
