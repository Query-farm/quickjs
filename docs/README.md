<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width="250" />
</p>

# DuckDB QuickJS Extension

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/quickjs.html)
[![v1.5 build](https://github.com/Query-farm/quickjs/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/quickjs/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

This extension provides an embedded [QuickJS-NG](https://github.com/quickjs-ng/quickjs) engine for [DuckDB](https://duckdb.org/). It allows you to execute JavaScript code directly within your SQL queries. QuickJS is a small, fast, and embeddable JavaScript engine that supports modern JavaScript features including ES2020.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/quickjs](https://query.farm/products/extensions/quickjs)**

## Installation

```sql
INSTALL quickjs FROM community;
LOAD quickjs;
```

## Development

For instructions on building the extension from source and running its tests, see [BUILDING.md](BUILDING.md).
