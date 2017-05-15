#pragma once

#include "SimpleAmqpClient/SimpleAmqpClient.h"

//#include "rustfp/result.h"
//#include "rustfp/unit.h"

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
        //! Acknowledge the message.
        ack,

        //! Reject acknowledging the message.
        rej,
    };

    /**
     * Holds the set of publisher and consumer channels.
     */
    class dual_channel
    {
    public:
        using consume_fn_t = std::function<ack(const char[], size_t)>;

        /**
         * Creates a set of publisher and consumer channels.
         *
         * The created queue will be durable, non-exclusive and does not
         * get auto-deleted.
         * @param queue queue name to create
         * @param hostname hostname to connect for AMQP service
         * @param port port number to connect for AMQP service
         */
        DLL_EASYAMQP dual_channel(
            const std::string &queue,
            const consume_fn_t &consume_fn,
            const std::chrono::milliseconds &consume_timeout = std::chrono::milliseconds(3000),
            const uint32_t thread_count = 1,
            const std::string &hostname = DEFAULT_HOSTNAME,
            const int port = DEFAULT_PORT);

        DLL_EASYAMQP ~dual_channel();

        // DLL_EASYAMQP auto publish(const char bytes[], const size_t len) -> ::rustfp::Result<::rustfp::unit_t, std::unique_ptr<std::exception>>;
        DLL_EASYAMQP void publish(const char bytes[], const size_t len);

    private:
        ::boost::shared_ptr<AmqpClient::Channel> c;
        std::shared_ptr<std::atomic_bool> is_running;
        std::thread t;
    };
}