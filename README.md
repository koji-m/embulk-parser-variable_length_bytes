# Variable Length Bytes parser plugin for Embulk

Variable length bytes record parser plugin for Embulk.

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **columns**: Specify column name and type, range of bytes (array, required)
- **record_separator**: Hexadecimal representation of characters for record separator sequence (eg. `0x20`) or new line character(`CR`, `LF`, `CRLF`) (string, default: `LF`)
- **charset**: Character encoding (eg. `ISO-8859-1`, `UTF-8`) (string, default: `UTF-8`)
- **stop_on_invalid_record**: Stop bulk load transaction if a file includes invalid record (boolean, default: `false`)

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


## Install plugin

Currently the status of this plugin is work in progress.

To install manually.

```
$ git clone https://github.com/koji-m/embulk-parser-variable_length_bytes.git
$ cd embulk-parser-variable_length_bytes
$ ./gradlew package

# prepare config.yml file

$ embulk run -L path/to/embulk-parser-variable_length_bytes config.yml
```

