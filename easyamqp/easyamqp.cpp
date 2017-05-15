#include "easyamqp.hpp"

#include "SimpleAmqpClient/SimpleAmqpClient.h"

#include <atomic>
#include <memory>
#include <string>

// SimpleAmqpClient
using AmqpClient::Channel;
using AmqpClient::Envelope;

// std
using std::atomic_bool;
using std::make_shared;
using std::string;

namespace easyamqp
{
    dual_channel::dual_channel(
        const std::string &queue,
        const consume_fn_t &consume_fn,
        const std::chrono::milliseconds &consume_timeout,
        const uint32_t thread_count,
        const std::string &hostname,
        const int port) :

        c(Channel::Create(hostname, port)),

        // to indicate to consumer to stop
        is_running(make_shared<atomic_bool>(true)),

        // consumer thread
        t([consume_fn, consume_timeout, is_running = this->is_running, queue, c = Channel::Create(hostname, port)]
        {
            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);

            while (is_running->load())
            {
                Envelope::ptr_t env = nullptr;
                const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, consume_timeout.count());

                // no message => timeout
                if (has_msg && is_running->load())
                {
                    const auto body = env->Message()->Body();
                    const auto ack = consume_fn(body.data(), body.length());

                    if (ack == ack::ack)
                    {
                        c->BasicAck(env);
                    }
                    else
                    {
                        c->BasicReject(env, true);
                    }
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
}