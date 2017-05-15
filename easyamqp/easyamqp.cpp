#include "easyamqp.hpp"

#include "rustfp/result.h"
#include "rustfp/unit.h"

#include "SimpleAmqpClient/SimpleAmqpClient.h"

#include <atomic>
#include <exception>
#include <limits>
#include <memory>
#include <string>

// SimpleAmqpClient
using AmqpClient::BasicMessage;
using AmqpClient::Channel;
using AmqpClient::Envelope;

// rustfp
using rustfp::Err;
using rustfp::Ok;
using rustfp::Result;
using rustfp::Unit;

// std
using std::atomic_bool;
using std::exception;
using std::make_shared;
using std::make_unique;
using std::numeric_limits;
using std::string;
using std::unique_ptr;

namespace easyamqp
{
    dual_channel::dual_channel(
        const std::string &queue,
        const consume_fn_t &consume_fn,
        const std::chrono::milliseconds &consume_timeout,
        const uint32_t thread_count,
        const std::string &hostname,
        const int port) :

        queue(queue),
        c(Channel::Create(hostname, port)),

        // to indicate to consumer to stop
        is_running(make_shared<atomic_bool>(true)),

        // consumer thread
        t([consume_fn, consume_timeout, is_running = this->is_running, queue, c = Channel::Create(hostname, port)]
        {
            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);

            const auto timeout_ms = consume_timeout.count() >= numeric_limits<int32_t>::max()
                ? numeric_limits<int32_t>::max()
                : static_cast<int32_t>(consume_timeout.count());

            while (is_running->load())
            {
                Envelope::ptr_t env = nullptr;
                const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, timeout_ms);

                // no message => timeout
                if (has_msg && is_running->load())
                {
                    const auto ack_res =
                        [&consume_fn, &env]() -> Result<ack, unique_ptr<exception>>
                        {
                            try
                            {
                                return Ok(consume_fn(env->Message()->Body()));
                            }
                            catch (const exception &e)
                            {
                                return Err(make_unique<exception>(e));
                            }
                        }();

                    ack_res.match(
                        [&c, &env](const auto &ack)
                        {
                            if (ack == ack::ack)
                            {
                                c->BasicAck(env);
                            }
                            else
                            {
                                c->BasicReject(env, true);
                            }
                        },

                        [&c, &env](const auto &e)
                        {
                            c->BasicReject(env, true);
                        });
                }
            }
        })
    {
        c->DeclareQueue(queue, false, true, false, false);
    }

    dual_channel::~dual_channel()
    {
        is_running->store(false);

        if (t.joinable())
        {
            t.join();
        }
    }

    auto dual_channel::publish(const char bytes[], const size_t len) -> ::rustfp::Result<::rustfp::unit_t, std::unique_ptr<std::exception>>
    {
        return publish(string(bytes, bytes + len));
    }

    auto dual_channel::publish(const string &msg) -> ::rustfp::Result<::rustfp::unit_t, std::unique_ptr<std::exception>>
    {
        try
        {
            const auto basic_msg = BasicMessage::Create(msg);
            c->BasicPublish("", queue, basic_msg);
            return Ok(Unit);
        }
        catch (const exception &e)
        {
            return Err(make_unique<exception>(e));
        }
    }
}