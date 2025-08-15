# Test key/value database using Seastar library

Project implements the following parts:

1. Key/value database server  
   Implements a database server with REST API protocol.
2. Test client  
   Sequentially runs validation tests against server using the REST API.
3. Performance test client  
   Runs multiple REST API clients in parallel, testing the server throughput.


## REST API design

All APIs use HTTP POST requests with parameters passed via JSON request body.

1. Read key/value entry

Path: /v1/get  
Request body:  { "key" : "1111" }  
Returns data via reply body: { "key" : "1111", "value" : "abcd" }  
Returns HTTP code 200 on success, or 404 if key not found.

2. Create/update key/value entry

Path: /v1/set  
Request body: { "key" : "1111" }  
Returns HTTP code 200 on success, or 404 if key not found, reply body being always empty.

3. Delete key/value entry

Path: /v1/delete  
Request body: { "key" : "1111" }  
Returns HTTP code 200 on success, or 404 if key not found, reply body being always empty.

4. Query key/value entries

Path: /v1/query  
Request body: { "key" : "11" }  
Returns array of keys with matching key prefix: [ {"key" : "1111"}, {"key" : "1122"} ]
Returns HTTP code 200.

## Implementation

Initial code layout/compilation based on app template at https://github.com/denesb/seastar-app-stub  
HTTP server framework was based on Seastar's httpd example application.  
Both test and performance client were based on seawreck example code.

## Compiling

More details here: https://github.com/denesb/seastar-app-stub

docker build -t seastar-app-stub .  
docker run --rm -it -v $(pwd):/home/src seastar-app-stub  
make all  

## Running

Adapt your system for Seastar with:  
sudo sh -c 'echo 529248 > /proc/sys/fs/aio-max-nr'

Run test within your development container with:  
make test

## To-do

Implement entry cache with LRU eviction policy.  
Implement file based server storage using per-CPU core data sharding.