---
title: Shape
---

## Main Structure

The array shape is stored within the [Array Schema](./array_schema.md#array-schema-file)
file. If a shape is empty, only the version number and the empty flag are serialized to storage.

| **Field** | **Type** | **Description** |
| :--- | :--- | :--- |
| Version number | `uint32_t` | Shape version number |
| Empty | `bool` | Whether the shape has a representation(e.g. NDRectangle) set or not |
| Type | `uint8_t` | The type of shape stored in this file |
| NDRectangle | [MBR](./fragment.md#mbr) | A hyperrectangle defined using [1DRange](./fragment.md#mbr) items for each dimension |
