## diff-rdb

[![Clojars Project](https://img.shields.io/clojars/v/belit/diff-rdb.svg)](https://clojars.org/belit/diff-rdb)

Tools and APIs for finding, inspecting and fixing differences between data sets in relational databases.

Data sets may be gathered from different joins, schemas, databases or RDBMSs.

Diff-rdb relies entirely upon SQL result sets, it can compare anything user can express with SQL.

Columns marked for comparison must have the same names. Aliases can be used if names differ.

To compare large data sets efficiently, queries can be partitioned and executed in parallel.

Diffs can be inspected with GUI, can be saved to disk in various data formats or exposed via [core.async](https://github.com/clojure/core.async) channels.

## Status

Under development, **APIs may change**.

#### TODO:
- ~Core diff algorithm~
- IO, parallelization
- UI, fixing diffs

## Documentation
- [Installation](doc/installation.md)
- GUI manual
- [Using the APIs](doc/api.md)

## License
Two options are available:
<table>
  <thead align="center">
    <tr>
      <th width="50%">Free license</th>
      <th width="50%">Paid license</th>
    </tr>
  </thead>
  <tbody align="center">
    <tr>
      <td>For non-commercial use such as teaching, academic research, personal study and for open source projects</td>
      <td>For commercial and non-commercial use</td>
    </tr>
    <tr>
      <td><a href="https://github.com/belit-rs/diff-rdb/blob/master/LICENSE">Read the license</a></td>
      <td><a href="http://belit.co.rs/en/kontakt/">Contact us</a></td>
    </tr>
  </tbody>
</table>

Copyright © Belit d.o.o.