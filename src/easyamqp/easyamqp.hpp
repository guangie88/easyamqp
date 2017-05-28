/**
 * Contains easy to use AMQP structures for easy publish/consume pattern.
 * @author Chen Weiguang
*/

#pragma once

#include "SimpleAmqpClient/SimpleAmqpClient.h"

#include "mapbox/variant.hpp"

#include "rustfp/cycle.h"
#include "rustfp/find_map.h"
#include "rustfp/result.h"
#include "rustfp/unit.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "DLL_SETTINGS.h"

namespace easyamqp {

    // declaration section

    namespace details {
        static constexpr auto DEFAULT_HOSTNAME = "127.0.0.1";
        static constexpr auto DEFAULT_PORT = 5672;
        static constexpr auto DEFAULT_USERNAME = "guest";
        static constexpr auto DEFAULT_PASSWORD = "guest";
        static constexpr auto DEFAULT_VHOST = "/";

        static const std::chrono::milliseconds DEFAULT_DUR(1000);

        DLL_EASYAMQP auto convert_timeout(const std::chrono::milliseconds &timeout) -> int32_t;
    }

    /** Acknowledgement enumeration. */
    enum class ack {
        /** Acknowledge the message. */
        ack,

        /** Reject acknowledging the message. */
        rej,
    };

    /** Consumer function type to be used for direct consuming. */
    template <class T>
    using consume_fn_t = std::function<::rustfp::Option<T>(std::string)>;

    /** Generic error type alias. */
    using err_t = std::unique_ptr<std::exception>;

    /** Alias to the internal typed channel pointer */
    using channel_ptr_t = ::boost::shared_ptr<AmqpClient::Channel>;

    /** Timeout error structure. */
    struct timeout_t {};

    /**
     * Holds a variant type between timeout error and
     * the generic error.
     */
    class consume_for_error_t {
    public:
        /** Alias to the actual variant type used. */
        using var_err_t = ::mapbox::util::variant<timeout_t, err_t>;

        /**
         * Constructor to initialize the error value.
         * @param v variant error value, which can be of timeout_t, or err_t.
         */
        DLL_EASYAMQP explicit consume_for_error_t(var_err_t &&v);

        /**
         * Check if the error is of timeout.
         * @return true if error is of timeout, else false
         */
        DLL_EASYAMQP auto is_timeout() const -> bool;

        /**
         * Copy the exception error into another box and returns it.
         *
         * Make sure to check if !is_timeout() first before calling this.
         * Otherwise it will lead to undefined behaviour since the actual
         * type contained is not checked.
         * @return Boxed version of the exception error value.
         */
        DLL_EASYAMQP auto get_err_unchecked() const -> err_t;

    private:
        ::mapbox::util::variant<timeout_t, err_t> v;
    };

    /**
     * Holds the set of publisher and consumer channels.
     */
    class dual_channel {
    public:
        /** Consumer acknowledgement function type to be used in the consumer loop. */
        using consume_ack_fn_t = std::function<ack(std::string)>;

        /**
         * Create a set of publisher and consumer channels.
         *
         * The created queue will be durable, non-exclusive and does not
         * get auto-deleted.
         * @param queue queue name to create
         * @param consume_ack_fn consumer function that handles the received message
         * when available in queue.
         * @param consume_timeout consume wait timeout duration before
         * performing another blocking wait.
         * @param thread_count number of threads permitted to run at the same time
         * to handle the received messages.
         * @param hostname hostname to connect for AMQP service.
         * @param port port number to connect for AMQP service.
         * @param username username to connect for AMQP service.
         * @param password password to connect for AMQP service.
         * @param vhost virtual host path to connect for AMQP service.
         */
        DLL_EASYAMQP dual_channel(
            const std::string &queue,
            const consume_ack_fn_t &consume_ack_fn,
            const std::chrono::milliseconds &consume_timeout = details::DEFAULT_DUR,
            const uint32_t thread_count = 1,
            const std::string &hostname = details::DEFAULT_HOSTNAME,
            const int port = details::DEFAULT_PORT,
            const std::string &username = details::DEFAULT_USERNAME,
            const std::string &password = details::DEFAULT_PASSWORD,
            const std::string &vhost = details::DEFAULT_VHOST);

        /**
         * Destroy the channel and connection, and gracefully terminate the consumer loop.
         *
         * This immediately prevents the consumer loop from accepting any remaining messages.
         */
        DLL_EASYAMQP ~dual_channel();

        /**
         * Publish the given bytes with specified length to the queue.
         * @param bytes char array of binary data to be sent over.
         * @param len number of bytes to be sent over.
         * @return Ok<unit_t> describes no error,
         * while Err<exception> holds the thrown exception while publishing.
         */
        DLL_EASYAMQP auto publish(const char bytes[], const size_t len) noexcept
            -> ::rustfp::Result<::rustfp::unit_t, err_t>;

        /**
         * Publish the given string to the queue.
         * @param msg string content to be sent over.
         * @return Ok<unit_t> describes no error,
         * while Err<exception> holds the thrown exception while publishing.
         */
        DLL_EASYAMQP auto publish(const std::string &msg) noexcept
            -> ::rustfp::Result<::rustfp::unit_t, err_t>;

    private:
        std::string queue;
        channel_ptr_t c;
        std::shared_ptr<std::atomic_bool> is_running;
        std::thread t;
    };

    /**
     * Publish the given bytes with specified length to the queue with the given name.
     *
     * The created queue will be durable, non-exclusive and does not
     * get auto-deleted.
     * @param queue queue name to create
     * @param bytes char array of binary data to be sent over.
     * @param len number of bytes to be sent over.
     * @param hostname hostname to connect for AMQP service.
     * @param port port number to connect for AMQP service.
     * @param username username to connect for AMQP service.
     * @param password password to connect for AMQP service.
     * @param vhost virtual host path to connect for AMQP service.
     * @return Ok<unit_t> describes no error,
     * while Err<exception> holds the thrown exception while publishing.
     */
    DLL_EASYAMQP auto publish(
        const std::string &queue,
        const char bytes[],
        const size_t len,
        const std::string &hostname = details::DEFAULT_HOSTNAME,
        const int port = details::DEFAULT_PORT,
        const std::string &username = details::DEFAULT_USERNAME,
        const std::string &password = details::DEFAULT_PASSWORD,
        const std::string &vhost = details::DEFAULT_VHOST) noexcept
        -> ::rustfp::Result<::rustfp::unit_t, err_t>;

    /**
     * Publish given message to the queue with the given name.
     *
     * The created queue will be durable, non-exclusive and does not
     * get auto-deleted.
     * @param queue queue name to create
     * @param msg string content to be sent over.
     * @param hostname hostname to connect for AMQP service.
     * @param port port number to connect for AMQP service.
     * @param username username to connect for AMQP service.
     * @param password password to connect for AMQP service.
     * @param vhost virtual host path to connect for AMQP service.
     * @return Ok<unit_t> describes no error,
     * while Err<exception> holds the thrown exception while publishing.
     */
    DLL_EASYAMQP auto publish(
        const std::string &queue,
        const std::string &msg,
        const std::string &hostname = details::DEFAULT_HOSTNAME,
        const int port = details::DEFAULT_PORT,
        const std::string &username = details::DEFAULT_USERNAME,
        const std::string &password = details::DEFAULT_PASSWORD,
        const std::string &vhost = details::DEFAULT_VHOST) noexcept
        -> ::rustfp::Result<::rustfp::unit_t, err_t>;

    template <class ConsumeFn>
    auto consume_for(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::chrono::milliseconds &consume_timeout = details::DEFAULT_DUR,
        const std::string &hostname = details::DEFAULT_HOSTNAME,
        const int port = details::DEFAULT_PORT,
        const std::string &username = details::DEFAULT_USERNAME,
        const std::string &password = details::DEFAULT_PASSWORD,
        const std::string &vhost = details::DEFAULT_VHOST) noexcept
        -> ::rustfp::Result<std::result_of_t<ConsumeFn(std::string)>, consume_for_error_t>;

    template <class ConsumeFn>
    auto consume(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::string &hostname = details::DEFAULT_HOSTNAME,
        const int port = details::DEFAULT_PORT,
        const std::string &username = details::DEFAULT_USERNAME,
        const std::string &password = details::DEFAULT_PASSWORD,
        const std::string &vhost = details::DEFAULT_VHOST) noexcept
        -> ::rustfp::Result<std::result_of_t<ConsumeFn(std::string)>, err_t>;

    // implementation section

    template <class ConsumeFn>
    auto consume_for(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::chrono::milliseconds &consume_timeout,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) noexcept
        -> ::rustfp::Result<std::result_of_t<ConsumeFn(std::string)>, consume_for_error_t> {

        try {
            auto c = AmqpClient::Channel::Create(hostname, port, username, password, vhost);
            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);

            AmqpClient::Envelope::ptr_t env = nullptr;
            const auto timeout_ms = details::convert_timeout(consume_timeout); 
            const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, timeout_ms);

            if (has_msg) {
                auto consume_opt = consume_fn(env->Message()->Body());

                if (consume_opt.is_some()) {
                    c->BasicAck(env);
                }
                else {
                    c->BasicReject(env, true);
                }

                return ::rustfp::Ok(std::move(consume_opt));
            } else {
                return ::rustfp::Err(consume_for_error_t{timeout_t{}});
            }
        }
        catch (const std::exception &e) {
            return ::rustfp::Err(consume_for_error_t(std::make_unique<std::exception>(e)));
        }
    }

    template <class ConsumeFn>
    auto consume(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) noexcept
        -> ::rustfp::Result<std::result_of_t<ConsumeFn(std::string)>, err_t> {

        using opt_t = std::result_of_t<ConsumeFn(std::string)>;
        using res_t = ::rustfp::Result<opt_t, err_t>;

        auto consume_res_opt = ::rustfp::cycle(::rustfp::Unit)
            | ::rustfp::find_map([&queue, &consume_fn, &hostname, port,
                &username, &password, &vhost](::rustfp::unit_t) -> ::rustfp::Option<res_t> {
                
                auto res = consume_for(queue, consume_fn, details::DEFAULT_DUR, hostname,
                    port, username, password, vhost);

                return std::move(res).match(
                    [](opt_t &&v) {
                        return ::rustfp::Some(res_t(::rustfp::Ok(std::move(v))));
                    },

                    [](consume_for_error_t &&e) -> ::rustfp::Option<res_t> {
                        if (e.is_timeout()) {
                            return ::rustfp::None;
                        }
                        else {
                            return ::rustfp::Some(res_t(::rustfp::Err(e.get_err_unchecked())));
                        }
                    });
            });

        // since it is a cycle + find_map, it is guaranteed to find Some
        assert(consume_res_opt.is_some());

        return std::move(consume_res_opt).unwrap_unchecked();
    }
}
