<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

[![](site/docs/img/Iceberg-logo.png)][apache-site]

[Apache Iceberg][apache-site] is an open table format for huge analytic datasets. Iceberg adds tables to Presto and Spark that use a high-performance format that works just like a SQL table.

To contribute to Iceberg, use the [Apache git repository][apache-repo] and [issue tracker][apache-issues].

This repository contains the Netflix branch of Apache Iceberg. The purpose of this repository is to share non-Iceberg additions with the community. Don't expect to be able to build this project.

Additions that are not part of Apache Iceberg in this repository include:

* An Iceberg catalog that uses Metacat
* A Spark catalog using Iceberg, Spark, and Hive tables
* A Spark ViewCatalog implementation
* A Spark DSv2 TableCatalog implementation for Spark and Hive tables

[apache-site]: https://iceberg.apache.org
[apache-repo]: https://github.com/apache/incubator-iceberg
[apache-issues]: https://github.com/apache/incubator-iceberg/issues


### Disclaimer

This repository depends on Netflix libraries. Do not expect this project to build.

