# Data Prep

![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)
![cdap-transform](https://cdap-users.herokuapp.com/assets/cdap-transform.svg)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=0)
[![Build Status](https://travis-ci.org/hydrator/wrangler.svg?branch=develop)](https://travis-ci.org/hydrator/wrangler)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/11434/badge.svg)](https://scan.coverity.com/projects/hydrator-wrangler-transform)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A collection of libraries, a pipeline plugin, and a CDAP service for performing data
cleansing, transformation, and filtering using a set of data manipulation instructions
(directives). These instructions are either generated using an interative visual tool or
are manually created.

Data Prep defines few concepts that might be useful if you are just getting started with it.
 Learn about them [here](docs/concepts.md)

The Data Prep Transform is [separately documented](transform/docs/data-prep-transform.md).

## Demo Videos and Recipes

More Videos [here](https://www.youtube.com/playlist?list=PLhmsf-NvXKJn-neqefOrcl4n7zU4TWmIr)

* Videos
  * [SCREENCAST] [Creating Lookup Dataset and Joining](https://www.youtube.com/watch?v=Nc1b0rsELHQ)
  * [SCREENCAST] [Restricted Directives](https://www.youtube.com/watch?v=71EcMQU714U)
  * [SCREENCAST] [Parse Excel files in CDAP](https://www.youtube.com/watch?v=su5L1noGlEk)
  * [SCREENCAST] [Parse File As AVRO File](https://www.youtube.com/watch?v=tmwAw4dKUNc)
  * [SCREENCAST] [Parsing Binary Coded AVRO Messages](https://www.youtube.com/watch?v=Ix_lPo-PDJY)
  * [SCREENCAST] [Parsing Binary Coded AVRO Messages & Protobuf messages using schema registry](https://www.youtube.com/watch?v=LVLIdWnUX1k)
  * [SCREENCAST] [Quantize a column - Digitize](https://www.youtube.com/watch?v=VczkYX5SRtY)
  * [SCREENCAST] [Data Cleansing capability with send-to-error directive](https://www.youtube.com/watch?v=aZd5H8hIjDc)
  * [SCREENCAST] [Building Data Prep from the GitHub source](https://youtu.be/pGGjKU04Y38)
  * [VOICE-OVER] [End-to-End Demo Video](https://youtu.be/AnhF0qRmn24)
  * [SCREENCAST] [Ingesting into Kudu](https://www.youtube.com/watch?v=KBW7a38vlUM)
  * [SCREENCAST] [Realtime HL7 CCDA XML from Kafka into Time Parititioned Parquet](https://youtu.be/0fqNmnOnD-0)
  * [SCREENCAST] [Parsing JSON file](https://youtu.be/vwnctcGDflE)
  * [SCREENCAST] [Flattening arrays](https://youtu.be/SemHxgBYIsY)
  * [SCREENCAST] [Data cleansing with send-to-error directive](https://www.youtube.com/watch?v=aZd5H8hIjDc)
  * [SCREENCAST] [Publishing to Kafka](https://www.youtube.com/watch?v=xdc8pvvlI48)
  * [SCREENCAST] [Fixed length to JSON](https://www.youtube.com/watch?v=3AXu4m1swuM)


* Recipes
  * [Parsing Apache Log Files](demos/parsing-apache-log-files.md)
  * [Parsing CSV Files and Extracting Column Values](demos/parsing-csv-extracting-column-values.md)
  * [Parsing HL7 CCDA XML Files](demos/parsing-hl7-ccda-xml-files.md)

## Available Directives

These directives are currently available:

| Directive                                                              | Description                                                      |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **Parsers**                                                            |                                                                  |
| [JSON Path](docs/directives/json-path.md)                              | Uses a DSL (a JSON path expression) for parsing JSON records     |
| [Parse as AVRO](docs/directives/parse-as-avro.md)                      | Parsing an AVRO encoded message - either as binary or json       |
| [Parse as AVRO File](docs/directives/parse-as-avro-file.md)            | Parsing an AVRO data file                                        |
| [Parse as CSV](docs/directives/parse-as-csv.md)                        | Parsing an input record as comma-separated values                |
| [Parse as Date](docs/directives/parse-as-date.md)                      | Parsing dates using natural language processing                  |
| [Parse as Excel](docs/directives/parse-as-excel.md)                    | Parsing excel file.                                              |
| [Parse as Fixed Length](docs/directives/parse-as-fixed-length.md)      | Parses as a fixed length record with specified widths            |
| [Parse as HL7](docs/directives/parse-as-hl7.md)                        | Parsing Health Level 7 Version 2 (HL7 V2) messages               |
| [Parse as JSON](docs/directives/parse-as-json.md)                      | Parsing a JSON object                                            |
| [Parse as Log](docs/directives/parse-as-log.md)                        | Parses access log files as from Apache HTTPD and nginx servers   |
| [Parse as Protobuf](docs/directives/parse-as-log.md)                   | Parses an Protobuf encoded in-memory message using descriptor    |
| [Parse as Simple Date](docs/directives/parse-as-simple-date.md)        | Parses date strings                                              |
| [Parse as XML](docs/directives/parse-as-xml.md)                        | Parses an XML document                                           |
| [Parse XML To JSON](docs/directives/parse-xml-to-json.md)              | Parses an XML document into a JSON structure                     |
| [XPath](docs/directives/xpath.md)                                      | Navigate the XML elements and attributes of an XML document      |
| **Output Formatters**                                                  |                                                                  |
| [Write as CSV](docs/directives/write-as-csv.md)                        | Converts a record into CSV format                                |
| [Write as JSON](docs/directives/write-as-json-map.md)                  | Converts the record into a JSON map                              |
| [Write JSON Object](docs/directives/write-as-json-object.md)           | Composes a JSON object based on the fields specified.            |
| **Transformations**                                                    |                                                                  |
| [Changing Case](docs/directives/changing-case.md)                      | Changes the case of column values                                |
| [Cut Character](docs/directives/cut-character.md)                      | Selects parts of a string value                                  |
| [Set Column](docs/directives/set-column.md)                            | Sets the column value to the result of an expression execution   |
| [Find and Replace](docs/directives/find-and-replace.md)                | Transforms string column values using a "sed"-like expression    |
| [Index Split](docs/directives/index-split.md)                          | (_Deprecated_)                                                   |
| [Invoke HTTP](docs/directives/invoke-http.md)                          | Invokes an HTTP Service (_Experimental_, potentially slow)       |
| [Quantization](docs/directives/quantize.md)                            | Quantizes a column based on specified ranges                     |
| [Regex Group Extractor](docs/directives/extract-regex-groups.md)       | Extracts the data from a regex group into its own column         |
| [Setting Character Set](docs/directives/set-charset.md)                | Sets the encoding and then converts the data to a UTF-8 String   |
| [Setting Record Delimiter](docs/directives/set-record-delim.md)        | Sets the record delimiter                                        |
| [Split by Separator](docs/directives/split-by-separator.md)            | Splits a column based on a separator into two columns            |
| [Split Email Address](docs/directives/split-email.md)                  | Splits an email ID into an account and its domain                |
| [Split URL](docs/directives/split-url.md)                              | Splits a URL into its constituents                               |
| [Text Distance (Fuzzy String Match)](docs/directives/text-distance.md) | Measures the difference between two sequences of characters      |
| [Text Metric (Fuzzy String Match)](docs/directives/text-metric.md)     | Measures the difference between two sequences of characters      |
| [URL Decode](docs/directives/url-decode.md)                            | Decodes from the `application/x-www-form-urlencoded` MIME format |
| [URL Encode](docs/directives/url-encode.md)                            | Encodes to the `application/x-www-form-urlencoded` MIME format   |
| [Trim](docs/directives/trim.md)                                        | Functions for trimming white spaces around string data           |
| **Encoders and Decoders**                                              |                                                                  |
| [Decode](docs/directives/decode.md)                                    | Decodes a column value as one of `base32`, `base64`, or `hex`    |
| [Encode](docs/directives/encode.md)                                    | Encodes a column value as one of `base32`, `base64`, or `hex`    |
| **Unique ID**                                                          |                                                                  |
| [UUID Generation](docs/directives/generate-uuid.md)                    | Generates a universally unique identifier (UUID)                 |
| **Date Transformations**                                               |                                                                  |
| [Diff Date](docs/directives/diff-date.md)                              | Calculates the difference between two dates                      |
| [Format Date](docs/directives/format-date.md)                          | Custom patterns for date-time formatting                         |
| [Format Unix Timestamp](docs/directives/format-unix-timestamp.md)      | Formats a UNIX timestamp as a date                               |
| **Lookups**                                                            |                                                                  |
| [Catalog Lookup](docs/directives/catalog-lookup.md)                    | Static catalog lookup of ICD-9, ICD-10-2016, ICD-10-2017 codes   |
| [Table Lookup](docs/directives/table-lookup.md)                        | Performs lookups into Table datasets                             |
| **Hashing & Masking**                                                  |                                                                  |
| [Message Digest or Hash](docs/directives/hash.md)                      | Generates a message digest                                       |
| [Mask Number](docs/directives/mask-number.md)                          | Applies substitution masking on the column values                |
| [Mask Shuffle](docs/directives/mask-shuffle.md)                        | Applies shuffle masking on the column values                     |
| **Row Operations**                                                     |                                                                  |
| [Filter Row if Matched](docs/directives/filter-row-if-matched.md)      | (_Deprecated_)                                                   |
| [Filter Row if True](docs/directives/filter-row-if-true.md)            | (_Deprecated_)                                                   |
| [Filter Rows On](docs/directives/filter-rows-on.md)                    | Filters records based on a condition                             |
| [Flatten](docs/directives/flatten.md)                                  | Separates the elements in a repeated field                       |
| [Fail on condition](docs/directives/fail.md)                           | Fails processing when the condition is evaluated to true.        |
| [Send to Error](docs/directives/send-to-error.md)                      | Filtering of records to an error collector                       |
| [Split to Rows](docs/directives/split-to-rows.md)                      | Splits based on a separator into multiple records                |
| **Column Operations**                                                  |                                                                  |
| [Change Column Case](docs/directives/change-column-case.md)            | Changes column names to either lowercase or uppercase            |
| [Changing Case](docs/directives/changing-case.md)                      | Change the case of column values                                 |
| [Cleanse Column Names](docs/directives/cleanse-column-names.md)        | Sanatizes column names, following specific rules                 |
| [Columns Replace](docs/directives/columns-replace.md)                  | Alters column names in bulk                                      |
| [Copy](docs/directives/copy.md)                                        | Copies values from a source column into a destination column     |
| [Drop Column](docs/directives/drop.md)                                 | Drops a column in a record                                       |
| [Fill Null or Empty Columns](docs/directives/fill-null-or-empty.md)    | Fills column value with a fixed value if null or empty           |
| [Keep Columns](docs/directives/keep.md)                                | Keeps specified columns from the record                          |
| [Merge Columns](docs/directives/merge.md)                              | Merges two columns by inserting a third column                   |
| [Rename Column](docs/directives/rename.md)                             | Renames an existing column in the record                         |
| [Set Column Names](docs/directives/set-columns.md)                     | Sets the names of columns, in the order they are specified       |
| [Split to Columns](docs/directives/split-to-columns.md)                | Splits a column based on a separator into multiple columns       |
| [Swap Columns](docs/directives/swap.md)                                | Swaps column names of two columns                                |
| [Set Column Data Type](docs/directives/set-type.md)                    | Convert data type of a column                                    |
| **NLP**                                                                |                                                                  |
| [Stemming Tokenized Words](docs/directives/stemming.md)                | Applies the Porter stemmer algorithm for English words           |
| **Transient Aggregators & Setters**                                    |                                                                  |
| [Increment Variable](docs/directives/increment-variable.md)            | Increments a transient variable with a record of processing.     |
| [Set Variable](docs/directives/set-variable.md)                        | Sets a transient variable with a record of processing.     |
| **Functions**                                                          |                                                                  |
| [Data Quality](docs/functions/dq-functions.md)                         | Data quality check functions. Checks for date, time, etc.        |
| [Date Manipulations](docs/functions/date-functions.md)                 | Functions that can manipulate date                               |
| [DDL](docs/functions/ddl-functions.md)                                 | Functions that can manipulate definition of data                 |
| [JSON](docs/functions/json-functions.md)                               | Functions that can be useful in transforming your data           |
| [Types](docs/functions/type-functions.md)                              | Functions for detecting the type of data                         |

## Restricting and Aliasing

A new capability that allows CDAP Administrators to restrict the directives that are accessible to their users.
More information on configuring can be found [here](docs/exclusion-and-aliasing.md)

## Performance

Initial performance tests show that with a set of directives of medium complexity for
transforming data, *DataPrep* is able to process at about 60K records per second. The
rates below are specified as *records/second*. Additional details and test results
[are available](docs/performance.md).

| Directive Complexity | Column Count |    Records |           Size | Mean Rate | 1 Minute Rate | 5 Minute Rate | 15 Minute Rate |
| -------------------- | :----------: | ---------: | -------------: | --------: | ------------: | ------------: | -------------: |
| Medium               |      18      | 13,499,973 |  4,499,534,313 | 64,998.50 |     64,921.29 |     46,866.70 |      36,149.86 |
| Medium               |      18      | 80,999,838 | 26,997,205,878 | 62,465.93 |     62,706.39 |     60,755.41 |      56,673.32 |


## Contact

### Mailing Lists

CDAP User Group and Development Discussions:

* [cdap-user@googlegroups.com](https://groups.google.com/d/forum/cdap-user)

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

### IRC Channel

CDAP IRC Channel: [#cdap on irc.freenode.net](http://webchat.freenode.net?channels=%23cdap)

### Slack Team

CDAP Users on Slack: [cdap-users team](https://cdap-users.herokuapp.com)


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
