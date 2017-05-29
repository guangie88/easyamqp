#include "easyamqp.hpp"

#include "ctpl_stl.h"

#include "rustfp/result.h"
#include "rustfp/unit.h"

#include "SimpleAmqpClient/SimpleAmqpClient.h"

#include <atomic>
#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <string>

// ctpl_stl
using ctpl::thread_pool;

// rustfp
using rustfp::Err;
using rustfp::Ok;
using rustfp::Result;
using rustfp::Unit;

// SimpleAmqpClient
using AmqpClient::BasicMessage;
using AmqpClient::Channel;
using AmqpClient::Envelope;

// std
using std::atomic_bool;
using std::exception;
using std::make_shared;
using std::make_unique;
using std::numeric_limits;
using std::string;
using std::unique_ptr;

namespace easyamqp {
    namespace details {
        auto convert_timeout(const std::chrono::milliseconds &timeout) -> int32_t {
            return timeout.count() >= numeric_limits<int32_t>::max()
                ? numeric_limits<int32_t>::max()
                : static_cast<int32_t>(timeout.count());
        }

        auto publish_impl(
            channel_ptr_t &c,
            const std::string &queue,
            const std::string &msg) noexcept
            -> ::rustfp::Result<::rustfp::unit_t, err_t> {

            try {
                const auto basic_msg = BasicMessage::Create(msg);
                c->BasicPublish("", queue, basic_msg);
                return Ok(Unit);
            }
            catch (const exception &e) {
                return Err(make_unique<exception>(e));
            }
        }
    }

    consume_for_error_t::consume_for_error_t(var_err_t &&v) :
        v(move(v)) {

    }

    auto consume_for_error_t::is_timeout() const -> bool {
        return v.is<timeout_t>();
    }

    auto consume_for_error_t::get_err_unchecked() const -> err_t {
        return make_unique<exception>(*v.get_unchecked<err_t>());
    }

    dual_channel::dual_channel(
        const std::string &queue,
        const consume_ack_fn_t &consume_ack_fn,
        const std::chrono::milliseconds &consume_timeout,
        const uint32_t thread_count,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) :

        queue(queue),
        c(Channel::Create(hostname, port, username, password, vhost)),

        // to indicate to consumer to stop
        is_running(make_shared<atomic_bool>(true)),

        // consumer thread
        t([consume_ack_fn, consume_timeout, queue, thread_count,
            c = Channel::Create(hostname, port, username, password, vhost),
            is_running = this->is_running] {

            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);
            const auto timeout_ms = details::convert_timeout(consume_timeout);

            thread_pool pool(thread_count);

            while (is_running->load()) {
                Envelope::ptr_t env = nullptr;
                const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, timeout_ms);

                // no message => timeout
                if (has_msg && is_running->load()) {
                    pool.push([c, consume_ack_fn, env](const int id) {
                        // wraps a try-catch block to play safe when calling external function
                        const auto ack_res =
                            [&consume_ack_fn, &env]() -> Result<ack, unique_ptr<exception>> {
                                try {
                                    return Ok(consume_ack_fn(env->Message()->Body()));
                                }
                                catch (const exception &e){
                                    return Err(make_unique<exception>(e));
                                }
                            }();

                        ack_res.match(
                            [&c, &env](const auto &ack) {
                                if (ack == ack::ack) {
                                    c->BasicAck(env);
                                }
                                else {
                                    c->BasicReject(env, true);
                                }
                            },

                            [&c, &env](const auto &e) {
                                c->BasicReject(env, true);
                            });
                    });
                }
            }
        })
    {
        c->DeclareQueue(queue, false, true, false, false);
    }

    dual_channel::~dual_channel() {
        is_running->store(false);

        if (t.joinable()) {
            t.join();
        }
    }

    auto dual_channel::publish(const char bytes[], const size_t len) noexcept
        -> ::rustfp::Result<::rustfp::unit_t, std::unique_ptr<std::exception>> {

        return publish(string(bytes, bytes + len));
    }

    auto dual_channel::publish(const string &msg) noexcept
        -> ::rustfp::Result<::rustfp::unit_t, std::unique_ptr<std::exception>> {

        return details::publish_impl(c, queue, msg);
    }

    auto publish(
        const std::string &queue,
        const char bytes[],
        const size_t len,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) noexcept
        -> ::rustfp::Result<::rustfp::unit_t, err_t> {

        return publish(queue, string(bytes, bytes + len));
    }

    DLL_EASYAMQP auto publish(
        const std::string &queue,
        const std::string &msg,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) noexcept
        -> ::rustfp::Result<::rustfp::unit_t, err_t> {

        try {
            auto c = Channel::Create(hostname, port, username, password, vhost);
            return details::publish_impl(c, queue, msg);
        }
        catch (const exception &e) {
            return Err(make_unique<exception>(e));
        }
    }
}
