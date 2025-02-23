# Aolwas Data tools

Toy project around Rust/Arrow/Datafusion/DeltaKernel-RS

Main focus are:

- understand Datafusion engine and how to integrate it in a data oriented project
- provide simple cli tool to preview in terminal different kinds of tabular data formats (parquet, csv, json, delta)
- play with delta-kernel-rs to build a custom table provider

In the future, the following features could be added:

- rest/flight api
- delta-sharing support
- iceberg support
- ...

There is no plan to make a prod ready tool, it is just a personal playground.

I clearly took inspiration from those projects:

* <https://github.com/timvw/qv>
* <https://github.com/roapi/roapi>
* <https://github.com/datafusion-contrib/datafusion-tui>
* <https://github.com/andygrove/bdt>
* <https://github.com/spiceai/spiceai>
