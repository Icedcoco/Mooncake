#include "http_metadata_server.h"

#include <csignal>
#include <ylt/coro_http/coro_http_server.hpp>
#include <glog/logging.h>

#include <mutex>
#include <string>

namespace mooncake {

HttpMetadataServer::HttpMetadataServer(uint16_t port, const std::string& host)
    : port_(port),
      host_(host),
      server_(std::make_unique<coro_http::coro_http_server>(4, port)),
      running_(false) {
    init_server();
}

HttpMetadataServer::~HttpMetadataServer() { stop(); }

void HttpMetadataServer::init_server() {
    using namespace coro_http;

    // GET /metadata?key=<key>
    server_->set_http_handler<GET>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::string key_string(key);
            std::string value;
            bool found = false;
            {
                std::lock_guard<std::mutex> lock(store_mutex_);
                auto it = store_.find(key_string);
                if (it != store_.end()) {
                    value = it->second;
                    found = true;
                }
            }

            if (!found) {
                LOG(WARNING) << "action=http_metadata_get, status=not_found, "
                             << "key=" << key_string;
                resp.set_status_and_content(status_type::not_found,
                                            "metadata not found");
                return;
            }

            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, value);
        });

    // PUT /metadata?key=<key>
    server_->set_http_handler<PUT>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::string body(req.get_body());
            std::string key_str(key);
            bool duplicate_rpc_meta = false;
            bool same_body = false;
            {
                std::lock_guard<std::mutex> lock(store_mutex_);
                if (key_str.find("rpc_meta") != std::string::npos) {
                    auto it = store_.find(key_str);
                    if (it != store_.end()) {
                        duplicate_rpc_meta = true;
                        same_body = it->second == body;
                    } else {
                        store_[key_str] = body;
                    }
                } else {
                    store_[key_str] = body;
                }
            }

            if (duplicate_rpc_meta) {
                LOG(WARNING)
                    << "action=http_metadata_put, "
                    << "status=duplicate_rpc_meta, "
                    << "same_body=" << (same_body ? "true" : "false")
                    << ", key=" << key_str;
                if (same_body) {
                    resp.set_status_and_content(status_type::ok,
                                                "metadata unchanged");
                    return;
                }
                resp.set_status_and_content(
                    status_type::bad_request,
                    "Duplicate rpc_meta key not allowed");
                return;
            }
            LOG(INFO) << "action=http_metadata_put, status=ok, key=" << key_str
                      << ", body_size=" << body.size();

            resp.set_status_and_content(status_type::ok, "metadata updated");
        });

    // DELETE /metadata?key=<key>
    server_->set_http_handler<coro_http::http_method::DEL>(
        "/metadata", [this](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            if (key.empty()) {
                resp.set_status_and_content(status_type::bad_request,
                                            "Missing key parameter");
                return;
            }

            std::string key_string(key);
            bool found = false;
            {
                std::lock_guard<std::mutex> lock(store_mutex_);
                auto it = store_.find(key_string);
                if (it != store_.end()) {
                    store_.erase(it);
                    found = true;
                }
            }

            if (!found) {
                LOG(WARNING) << "action=http_metadata_delete, "
                             << "status=not_found, key=" << key_string;
                resp.set_status_and_content(status_type::not_found,
                                            "metadata not found");
                return;
            }

            LOG(INFO) << "action=http_metadata_delete, status=ok, key="
                      << key_string;
            resp.set_status_and_content(status_type::ok, "metadata deleted");
        });

    // Health check endpoint
    server_->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.set_status_and_content(status_type::ok, "OK");
        });
}

bool HttpMetadataServer::start() {
    if (running_) {
        return true;
    }

    server_->async_start();
    running_ = true;
    LOG(INFO) << "HTTP metadata server started on " << host_ << ":" << port_;
    return true;
}

void HttpMetadataServer::stop() {
    if (!running_) {
        return;
    }

    server_->stop();
    running_ = false;
    LOG(INFO) << "HTTP metadata server stopped";
}

KVPoll HttpMetadataServer::poll() const {
    if (!running_) {
        return KVPoll::Failed;
    }
    return KVPoll::Success;
}

}  // namespace mooncake
