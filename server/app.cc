#include <memory>
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/core/print.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/http/reply.hh>
#include "stop_signal.hh"

#include "store_cache.hh"
#include "store_disk.hh"

namespace bpo = boost::program_options;

using namespace seastar;
using namespace httpd;
using namespace kvdb;

std::unique_ptr<database> g_db;

void extract_json_value(sstring data, std::string_view key, std::string &out) {
  const std::string pattern = fmt::format("\"{}\" : \"", key);
  size_t start = data.find(pattern);
  if (start != std::string::npos) {
    start += pattern.size();
    const size_t end = data.find("\"", start);
    if (end != std::string::npos) {
      out = data.substr(start, end-start);
      return;
    }
  }
  fmt::print("extract_json_value - failed\n");
}

class handle_get : public httpd::handler_base {
public:
    virtual future<std::unique_ptr<http::reply> > handle(const sstring& path,
            std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
        std::string key;
        extract_json_value(req->content, "key", key);
        //fmt::print("Server: handle get() content[{}], key [{}]\n", req->content, key);
        std::string value = co_await g_db->get(key);
        //fmt::print("Server: handle get() got value [{}]\n", value);
        if (value.empty()) {
		    rep->set_status(http::reply::status_type::not_found);  // 404
            rep->_skip_body = true;
		    rep->done();
        } else {
            std::string body = fmt::format("{{ \"key\" : \"{}\", \"value\" : \"{}\" }}", key, value);
            rep->_content = body;
		    rep->done("json");
        }
        co_return std::move(rep);
    }
};

class handle_set : public httpd::handler_base {
public:
    virtual future<std::unique_ptr<http::reply> > handle(const sstring& path,
            std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
        std::string key, value;
        extract_json_value(req->content, "key", key);
        extract_json_value(req->content, "value", value);
        co_await g_db->set(key, value);
        rep->_skip_body = true;
	    rep->done();
        co_return std::move(rep);
    }
};

class handle_del : public httpd::handler_base {
public:
    virtual future<std::unique_ptr<http::reply> > handle(const sstring& path,
            std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
        std::string key;
        extract_json_value(req->content, "key", key);
        bool success = co_await g_db->del(key);
        if (!success) {
		    rep->set_status(http::reply::status_type::not_found);  // 404
        }
        rep->_skip_body = true;
        rep->done();
        co_return std::move(rep);
    }
};

class handle_query : public httpd::handler_base {
public:
    virtual future<std::unique_ptr<http::reply> > handle(const sstring& path,
            std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
        std::string prefix;
        extract_json_value(req->content, "prefix", prefix);
        std::set<std::string> matches = co_await g_db->query(prefix);
        std::string body = "[ ";
        for (auto &match : matches) {
           if (body.size() > 2) body += ", ";
           body += fmt::format("{{ \"key\" : \"{}\" }}", match);
        }
        body += " ]";
        //fmt::print("QUERY HANDLER: body={}\n", body);
        rep->_content = body;
        rep->done();
        co_return std::move(rep);
    }
};

void set_routes(routes& r) {
    r.add(operation_type::POST, url("/v1/get"), new handle_get);
    r.add(operation_type::POST, url("/v1/set"), new handle_set);
    r.add(operation_type::POST, url("/v1/delete"), new handle_del);
    r.add(operation_type::POST, url("/v1/query"), new handle_query);
}

int main(int ac, char** av) {
    app_template app;

    return app.run(ac, av, [&] () -> future<int> {
        seastar_apps_lib::stop_signal stop_signal;

        // initialize database server with two layers:
        // - in-memory cache
        // - on-disk storage
        IStorage *cache = new CacheStorage(20);
        IStorage *disk = new DiskStorage();
        std::vector<IStorage *> store{ cache, disk };
        //std::vector<IStorage *> store{ disk };

        g_db = std::make_unique<database>(store);
        co_await g_db->start();

        http_server_control server;
        co_await server.start();
        co_await server.set_routes([](routes &r) { set_routes(r); });
        co_await server.listen(seastar::make_ipv4_address({10000}));

        co_await stop_signal.wait();
        co_await server.stop();
        co_await g_db->stop();

        co_return 0;
   });
}
