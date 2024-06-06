---
title: CurrentDomain
---

## Main Structure

The array CurrentDomain is stored within the [Array Schema](./array_schema.md#array-schema-file)
file. If a CurrentDomain is empty, only the version number and the empty flag are serialized to storage.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | CurrentDomain version number |
| Empty | `uint8_t` | Whether the CurrentDomain has a representation(e.g. NDRectangle) set or not |
| Type | `uint8_t` | The type of CurrentDomain stored in this file |
| NDRectangle | [MBR](./fragment.md#mbr) | A hyperrectangle defined using [1DRange](./fragment.md#mbr) items for each dimension |
