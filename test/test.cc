/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <seastar/http/response_parser.hh>
#include <seastar/net/api.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/print.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <chrono>

using namespace seastar;

struct test_info {
  std::string_view path;     // REST API path
  std::string_view body;     // REST API (POST) request body
  int res_code;              // expected response status code
  std::string_view res_body; // expected response body
} all_tests [] = {
 {"/v1/get",    "{ \"key\" : \"1111\" }", 404, ""},                                              // get - nonexistent key
 {"/v1/set",    "{ \"key\" : \"2222\", \"value\" : \"bbbb\" }", 200, ""},                        // set - key created
 {"/v1/get",    "{ \"key\" : \"2222\" }", 200, "{ \"key\" : \"2222\", \"value\" : \"bbbb\" }"},  // get - key exists
 {"/v1/delete", "{ \"key\" : \"1111\" }", 200, ""},                                              // delete - nonexistent key (succeeds too)
 {"/v1/set",    "{ \"key\" : \"2233\", \"value\" : \"cccc\" }", 200, ""},                        // set - another key stored
 {"/v1/query",  "{ \"prefix\" : \"22\" }",   200, "[ { \"key\" : \"2222\" }, { \"key\" : \"2233\" } ]"}, // query by key prefix
 {"/v1/delete", "{ \"key\" : \"2222\" }", 200, ""},                                              // delete - key found
 {"/v1/delete", "{ \"key\" : \"2222\" }", 200, ""},                                              // delete - nonexistent key key (already deleted)
 {"/v1/query",  "{ \"prefix\" : \"22\" }",   200, "[ { \"key\" : \"2233\" } ]"}                       // query by key prefix
};

template <typename T> bool runtime_assert_equal(const T &a, const T &b, size_t test_idx) {
  if (a != b) {
    fmt::print("Test #{} failed, [expected,result] values don't match!\n{}\n{}\n", test_idx, a, b);
    return false;
  }
  return true;
}

class http_client {
private:
    connected_socket _socket;
public:
    http_client() {}

    class connection {
    private:
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        http_response_parser _parser;
        http_client* _http_client;
    public:
        connection(connected_socket&& fd, http_client* client)
            : _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output())
            , _http_client(client){
        }

        future< std::tuple<std::string, int> > do_req(const struct test_info &t) {
            std::string request = fmt::format("POST {} HTTP/1.1\r\nHost: 127.0.0.1:10000\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", t.path, t.body.size(), t.body);
	        // fmt::print("HTTP request:\n[{}]\n", request);
            co_await _write_buf.write(request);
            co_await _write_buf.flush();
            _parser.init();
            co_await _read_buf.consume(_parser);

            // Read HTTP response header first
            if (_parser.eof()) {
               co_return std::make_tuple<std::string, int>("", -1);
            }
            auto _rsp = _parser.get_parsed_response();
            auto it = _rsp->_headers.find("Content-Length");
            if (it == _rsp->_headers.end()) {
               fmt::print("Error: HTTP response does not contain: Content-Length\n");
               co_return std::make_tuple<std::string, int>("", -1);
            }
            auto content_len = std::stoi(it->second);
            //fmt::print("Content-Length = {}\n", content_len);
            // Read HTTP response body
            std::string body;
            if (content_len) {
              seastar::temporary_buffer<char> buf = co_await _read_buf.read_exactly(content_len);
               //fmt::print("TEST got response: [{}]\n", buf.get());
               body.assign(buf.get(), content_len);  // important to clip the buffer!
            } else {
              // fmt::print("TEST got empty response body\n");
            }
            int code = (int)_rsp->_status;
            co_return std::make_tuple<std::string, int>(std::move(body), std::move(code));
        }
    };

    future<> connect(ipv4_addr server_addr) {
        connected_socket fd = co_await seastar::connect(make_ipv4_address(server_addr));
        _socket = std::move(fd);
        //http_debug("Established connection on cpu %3d\n", this_shard_id());
        co_return;
    }

    future<> run() {
        // All connected, start HTTP request
        auto conn = new connection(std::move(_socket), this);

        size_t test_idx = 0;
        for (auto &t : all_tests) {
          fmt::print("Test #{} start.\n", test_idx);
          auto [ data, code ] = co_await conn->do_req(t);

          // validate test results
          if(runtime_assert_equal(t.res_body, std::string_view(data), test_idx) &&
             runtime_assert_equal(t.res_code, code, test_idx)) {
            fmt::print("Test #{} succeeded!\n", test_idx);
          }
          ++ test_idx;

          co_await seastar::coroutine::maybe_yield();
        }

        delete conn;
        co_return;
    }
};

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    app_template::config app_cfg;
    app_cfg.auto_handle_sigint_sigterm = false;
    app_template app(std::move(app_cfg));

    app.add_options()
        ("server,s", bpo::value<std::string>()->default_value("127.0.0.1:10000"), "Server address");

    return app.run(ac, av, [&app] () -> future<int> {
        auto& config = app.configuration();
        auto server = config["server"].as<std::string>();

        fmt::print("========== http_client ============\n");
        fmt::print("Server: {}\n", server);

        // single HTTP client to sequentially run the k/v service validity tests
        http_client client;
        co_await client.connect(ipv4_addr{server});
        co_await client.run();
        fmt::print("==========     done     ============\n");
        co_return 0;
      });
}
