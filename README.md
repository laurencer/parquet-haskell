Parquet for Haskell
===================

WIP attempt to write a parser for Parquet.

NB. this is a sideproject and is still extremely rough (and reflects the exploratory style of development).

To run - copy the `customer.impala.parquet` file to the `dist/build/parquet` directory and then use `cabal run`. Right now it will spit out a bunch of metadata read.

TODO
----

- [ ] Properly handle errors (as opposed to coercing types, etc).
- [ ] Implement parsers for the common encoding formats (e.g. `RLE` and `BitPacking`) so we can decode the repetition and definition levels.
- [ ] Switch to a better data type than `Handle` (basically we need random IO). `Handle` has a few problems at the moment (e.g. unless you force the parsing using a bang - it can try and resolve it after you have already moved the pointer resulting in an exception: `parquet: thread blocked indefinitely in an MVar operation`).
- [ ] Write a more complete TODO list (...)

