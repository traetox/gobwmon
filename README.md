# gobwmon
Simple bandwidth monitor and tracking tool.  Basically watches usage on N number of interfaces and logs to a bolt database.

The data can then be queried back out via an HTTP API.  Also includes an integrated webserver, because golang.

A very basic web API can be found at https://github.com/traetox/bwmonfrontend

Just point the Web-Root variable at the clone location and you are off to the races.
