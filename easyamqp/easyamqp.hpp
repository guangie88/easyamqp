/**
 * Contains easy to use AMQP structures for easy publish/consume pattern.
 * @author Chen Weiguang
*/

#pragma once

#include "SimpleAmqpClient/SimpleAmqpClient.h"

#include "rustfp/result.h"
#include "rustfp/unit.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "DLL_SETTINGS.h"

namespace easyamqp
{
    // declaration section

    static constexpr auto DEFAULT_HOSTNAME = "127.0.0.1";
    static constexpr auto DEFAULT_PORT = 5672;

    /**
     * Acknowledgement enumeration.
     */
    enum class ack
    {
        /** Acknowledge the message. */
        ack,

        /** Reject acknowledging the message. */
        rej,
    };

    /**
     * Holds the set of publisher and consumer channels.
     */
    class dual_channel
    {
    public:
        /** Consumer function type to be used in the consumer loop. */
        using consume_fn_t = std::function<ack(std::string)>;

        /**
         * Creates a set of publisher and consumer channels.
         *
         * The created queue will be durable, non-exclusive and does not
         * get auto-deleted.
         * @param queue queue name to create
         * @param consume_fn consumer function that handles the received message when available in queue.
         * @param consume_timeout consume wait timeout duration before perform another blocking wait.
         * @param thread_count number of threads permitted to run at the same time to handle the received messages.
         * @param hostname hostname to connect for AMQP service.
         * @param port port number to connect for AMQP service.
         */
        DLL_EASYAMQP dual_channel(
            const std::string &queue,
            const consume_fn_t &consume_fn,
            const std::chrono::milliseconds &consume_timeout = std::chrono::milliseconds(1000),
            const uint32_t thread_count = 1,
            const std::string &hostname = DEFAULT_HOSTNAME,
            const int port = DEFAULT_PORT);

        /**
         * Destroys the channel and connection, and gracefully terminate the consumer loop.
         *
         * This immediately prevents the consumer loop from accepting any remaining messages.
         */
        DLL_EASYAMQP ~dual_channel();

        /**
         * Publishes the given bytes with specified length to the queue.
         * @param bytes char array of binary data to be sent over.
         * @param len number of bytes to be sent over.
         * @return Ok<unit_t> describes no error, while Err<exception> holds the thrown exception while publishing.
         */
        DLL_EASYAMQP auto publish(const char bytes[], const size_t len) -> ::rustfp::Result<::rustfp::unit_t, std::unique_ptr<std::exception>>;

        /**
         * Publishes the given string to the queue.
         * @param msg string content to be sent over.
         * @return Ok<unit_t> describes no error, while Err<exception> holds the thrown exception while publishing.
         */
        DLL_EASYAMQP auto publish(const std::string &msg) -> ::rustfp::Result<::rustfp::unit_t, std::unique_ptr<std::exception>>;

    private:
        std::string queue;
        ::boost::shared_ptr<AmqpClient::Channel> c;
        std::shared_ptr<std::atomic_bool> is_running;
        std::thread t;
    };
}