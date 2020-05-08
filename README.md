# Variable Length Bytes parser plugin for Embulk

[![Gem Version](https://badge.fury.io/rb/embulk-parser-variable_length_bytes.svg)](https://badge.fury.io/rb/embulk-parser-variable_length_bytes)

Variable length bytes record parser plugin for Embulk.

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Install

```shell script
$ embulk gem install embulk-parser-variable_length_bytes
```
## Configuration

- **columns**: Specify column name and type, range of bytes('\<start\>..\<end\>'). If the last column has a variable length, specify it as '\<start\>...'. (array, required)
- **record_separator**: Hexadecimal representation of characters for record separator sequence (eg. `0x20`) or new line character(`CR`, `LF`, `CRLF`). Set `null` to parse fixed-length records without record separator. (string, default: `LF`)
- **charset**: Character encoding (eg. `ISO-8859-1`, `UTF-8`). (string, default: `UTF-8`)
- **stop_on_invalid_record**: Stop bulk load transaction if a file includes invalid record. (boolean, default: `false`)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: variable_length_bytes
    charset: Shift_JIS
    record_separator: CRLF
    stop_on_invalid_record: true
    columns: 
    - {name: id, type: long, pos: '0..3'}
    - {name: name, type: string, pos: '3..11'}
    - {name: price, type: double, pos: '11..15'}
    - {name: flag, type: boolean, pos: '15..20'}
    - {name: description, type: string, pos: '20...'}
```

## Build

```shell script
$ ./gradlew gem
```

