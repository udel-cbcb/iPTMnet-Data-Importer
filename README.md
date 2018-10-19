# IPTMNET API
Welcome to the documentation of iPTMNet data importer. This tool supports importing data from CSV files into a postgres database.

Command line options

* `--host` : The address of postgres database. Default - localhost
* `--port` : The port on which postgres database is running. Default - 5432 
* `--user` : Username of the user that owns iptmnet database. Default - Postgres
* `--pass` : Password of the user that owns iptmnet database. Default - postgres

To create static builds

* Add to bash rc : `alias rust-musl-builder='docker run --rm -it -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder'`
* To build execute `./build.sh`
