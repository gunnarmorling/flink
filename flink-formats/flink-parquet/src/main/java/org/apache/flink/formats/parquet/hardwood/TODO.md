<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Hardwood-backed Parquet reader — follow-ups

## Support arbitrarily nested ARRAY / MAP / ROW

### Current limitation

`HardwoodFieldReader.create` only handles one level of nesting. It throws
`UnsupportedOperationException` for:

- **Nested arrays** — an `ARRAY` whose element is itself a group (`ARRAY<ARRAY<...>>`,
  `ARRAY<ROW<...>>`, `ARRAY<MAP<...>>`). See the "Nested arrays are not yet supported" guard.
- **Maps with non-primitive keys or values** — `MAP<K, ROW<...>>`, `MAP<K, ARRAY<...>>`, etc. See
  the "Nested map key/value types are not yet supported" guard.
- By extension, `ROW` fields whose members are any of the above.

Flat columns, single-level `ARRAY<primitive>`, `MAP<primitive, primitive>`, and `ROW` of those are
supported.

### Why it is unblocked

Hardwood's `ColumnReader` exposes the full layer chain between root and leaf
(`getLayerCount()`, `getLayerKind(layer)`, `getLayerOffsets(layer)`, `getLayerValidity(layer)`,
`getLeafValidity()`), where each enclosing group contributes one layer:

- `OPTIONAL` group → `STRUCT` layer
- `LIST` / `MAP`-annotated group → `REPEATED` layer
- `REQUIRED` group / synthetic LIST scaffolding → no layer

`HardwoodColumnVectorFiller` already consumes a single REPEATED layer (`fillArrayVector` /
`fillMapVector`) and a single STRUCT layer (`RowFieldReader`). Arbitrary nesting is expressible by
walking multiple layers per leaf rather than assuming a single `repeatedLayer` / `structLayer`.

### What needs to happen

- Generalize the `*FieldReader` subtree so a collection element / map value can itself be an
  `ARRAY` / `MAP` / `ROW` reader, recursing through the leaf's layer chain.
- Drive child collection vectors (`HeapArrayVector` / `HeapMapVector`) from the offsets of the
  appropriate inner REPEATED layer instead of the single outermost one; nested element nulls come
  from the inner layer's validity, leaf nulls from `getLeafValidity()`.
- Build the per-leaf layer indices during the schema walk in `HardwoodFieldReader.create`, the same
  way `parentLayerCount` is threaded today for the single-level case.

### Acceptance criteria

- `ARRAY<ARRAY<...>>`, `ARRAY<ROW<...>>`, `MAP<K, ROW<...>>`, `MAP<K, ARRAY<...>>` and `ROW`s
  composed of these read correctly, including null/empty distinction at every level.
- New cases added to `ParquetColumnarRowInputFormatTest` (or a dedicated nested-types test) with
  fixtures covering nulls and empties at each nesting level.
- The `UnsupportedOperationException` guards are removed.
