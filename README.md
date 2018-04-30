# ftpsuck

A recursive FTP downloader using asyncio.

Requires Python 3.6+.


## Installation

1. Clone the repo.
2. `pip install -r requirements.txt`


## Configuration

ftpsuck uses a configuration file like this:

```ini
[shortname]
host = ftp.example.com
username = ftp_username   # use anonymous for anonymous FTP
password = ftp_password   # use an e-mail address for anonymous FTP
remote_path = path/to/dir/
local_path = output/
pattern = *.csv   # defaults to all files
parallel = 5  # defaults to 1
```

Many such configurations can be included in each configuration file. Each one
is identified by its short name.

To download for a specific short name:

```
$ python ftpsuck.py -C <config> <shortname>
```
