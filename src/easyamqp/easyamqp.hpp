/**
 * Contains easy to use AMQP direct publish/consume functions.
 * All messages to send and receive are msgpack serializable for convenience.
 * @author Chen Weiguang
*/

#pragma once

#include "ctpl_stl.h"
#include "fn_traits.hpp"

#include "SimpleAmqpClient/SimpleAmqpClient.h"

#include "mapbox/variant.hpp"

#include "msgpack.hpp"

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
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

namespace easyamqp {

    // declaration section

    namespace details {
        static constexpr auto DEFAULT_HOSTNAME = "127.0.0.1";
        static constexpr auto DEFAULT_PORT = 5672;
        static constexpr auto DEFAULT_USERNAME = "guest";
        static constexpr auto DEFAULT_PASSWORD = "guest";
        static constexpr auto DEFAULT_VHOST = "/";

        static const std::chrono::milliseconds DEFAULT_DUR(1000);
    }

    /** Acknowledgement enumeration. */
    enum class ack {
        /** Acknowledge the message. */
        ack,

        /** Reject acknowledging the message. */
        rej,
    };

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
        explicit consume_for_error_t(var_err_t &&v);

        /**
         * Check if the error is of timeout.
         * @return true if error is of timeout, else false
         */
        auto is_timeout() const -> bool;

        /**
         * Copy the exception error into another box and returns it.
         *
         * Make sure to check if !is_timeout() first before calling this.
         * Otherwise it will lead to undefined behaviour since the actual
         * type contained is not checked.
         * @return Boxed version of the exception error value.
         */
        auto get_err_unchecked() const -> err_t;

    private:
        ::mapbox::util::variant<timeout_t, err_t> v;
    };
    
    /**
     * Holds the lifetime of the given consumer function
     * to consume in a push style.
     */
    class subscriber {
    public:
        /**
         * Create a set of publisher and consumer channels.
         *
         * The created queue will be durable, non-exclusive and does not
         * get auto-deleted.
         * @param queue queue name to create
         * @param consume_ack_fn consumer function that handles the deserialized value
         * when available in queue, and returns ack.
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
        template <class ConsumeAckFn>
        subscriber(
            const std::string &queue,
            const ConsumeAckFn &consume_ack_fn,
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
        ~subscriber();

    private:
        std::shared_ptr<std::atomic_bool> is_running;
        std::thread t;
    };

    /**
     * Publish given message to the queue with the given name.
     *
     * The created queue will be durable, non-exclusive and does not
     * get auto-deleted.
     * @param queue queue name to create
     * @param value msgpack serializable data to be sent over.
     * @param hostname hostname to connect for AMQP service.
     * @param port port number to connect for AMQP service.
     * @param username username to connect for AMQP service.
     * @param password password to connect for AMQP service.
     * @param vhost virtual host path to connect for AMQP service.
     * @return Ok<unit_t> describes no error,
     * while Err<exception> holds the thrown exception while publishing.
     */
    template <class T>
    auto publish(
        const std::string &queue,
        const T &value,
        const std::string &hostname = details::DEFAULT_HOSTNAME,
        const int port = details::DEFAULT_PORT,
        const std::string &username = details::DEFAULT_USERNAME,
        const std::string &password = details::DEFAULT_PASSWORD,
        const std::string &vhost = details::DEFAULT_VHOST) noexcept
        -> rustfp::Result<rustfp::unit_t, err_t>;

    /**
     * Consumes any given message in the queue, up to the timeout duration.
     *
     * The created queue will be durable, non-exclusive and does not
     * get auto-deleted.
     * @param queue queue name to create
     * @param consume_fn consumer function, takes in T and produces any Option.
     * Returning Some(_) acknowledges the message, while None rejects and requeues the message.
     * @param consume_timeout consumer wait timeout duration.
     * @param hostname hostname to connect for AMQP service.
     * @param port port number to connect for AMQP service.
     * @param username username to connect for AMQP service.
     * @param password password to connect for AMQP service.
     * @param vhost virtual host path to connect for AMQP service.
     * @return Ok<_> on success,
     * while Err<consume_for_error_t> holds the variant error value.
     */
    template <class T, class ConsumeFn>
    auto consume_for(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::chrono::milliseconds &consume_timeout = details::DEFAULT_DUR,
        const std::string &hostname = details::DEFAULT_HOSTNAME,
        const int port = details::DEFAULT_PORT,
        const std::string &username = details::DEFAULT_USERNAME,
        const std::string &password = details::DEFAULT_PASSWORD,
        const std::string &vhost = details::DEFAULT_VHOST) noexcept
        -> rustfp::Result<std::result_of_t<ConsumeFn(T &&)>, consume_for_error_t>;

    /**
     * Consumes any given message in the queue, waiting until the message arrives.
     *
     * The created queue will be durable, non-exclusive and does not
     * get auto-deleted.
     * @param queue queue name to create
     * @param consume_fn consumer function, takes in T and produces any Option.
     * Returning Some(_) acknowledges the message, while None rejects and requeues the message.
     * @param consume_timeout consumer wait timeout duration.
     * @param hostname hostname to connect for AMQP service.
     * @param port port number to connect for AMQP service.
     * @param username username to connect for AMQP service.
     * @param password password to connect for AMQP service.
     * @param vhost virtual host path to connect for AMQP service.
     * @return Ok<_> on success,
     * while Err<err_t> holds the exception error value.
     */
    template <class T, class ConsumeFn>
    auto consume(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::string &hostname = details::DEFAULT_HOSTNAME,
        const int port = details::DEFAULT_PORT,
        const std::string &username = details::DEFAULT_USERNAME,
        const std::string &password = details::DEFAULT_PASSWORD,
        const std::string &vhost = details::DEFAULT_VHOST) noexcept
        -> rustfp::Result<std::result_of_t<ConsumeFn(T &&)>, err_t>;

    // implementation section

    namespace details {
        inline auto convert_timeout(const std::chrono::milliseconds &timeout) -> int32_t {
            return timeout.count() >= std::numeric_limits<int32_t>::max()
                ? std::numeric_limits<int32_t>::max()
                : static_cast<int32_t>(timeout.count());
        }

        template <class T>
        auto pack(const T &value) noexcept -> rustfp::Result<std::string, std::unique_ptr<std::exception>> {
            try {
                std::stringstream buf;
                msgpack::pack(buf, value);
                return rustfp::Ok(buf.str());
            }
            catch (const std::exception &e) {
                return rustfp::Err(std::make_unique<std::exception>(e));
            }
        }

        template <class T>
        auto unpack(const std::string &str) noexcept -> rustfp::Result<T, std::unique_ptr<std::exception>> {
            static_assert(std::is_default_constructible<T>::value,
                "unpack<T> must have T that is default constructible!");

            static_assert(std::is_move_constructible<T>::value,
                "unpack<T> must have T that is move/copy constructible!");

            try {
                T value;

                const auto handle = msgpack::unpack(str.data(), str.size());
                const auto deserialized = handle.get();
                deserialized.convert(value);

                return rustfp::Ok(std::move(value));
            }
            catch (const std::exception &e) {
                return rustfp::Err(std::make_unique<std::exception>(e));
            }
        }
    }

    inline consume_for_error_t::consume_for_error_t(var_err_t &&v) :
        v(std::move(v)) {

    }

    inline auto consume_for_error_t::is_timeout() const -> bool {
        return v.is<timeout_t>();
    }

    inline auto consume_for_error_t::get_err_unchecked() const -> err_t {
        return std::make_unique<std::exception>(*v.get_unchecked<err_t>());
    }

    template <class ConsumeAckFn>
    subscriber::subscriber(
        const std::string &queue,
        const ConsumeAckFn &consume_ack_fn,
        const std::chrono::milliseconds &consume_timeout,
        const uint32_t thread_count,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) :

        // to indicate to stop
        is_running(std::make_shared<std::atomic_bool>(true)),

        // looping thread
        t([consume_ack_fn, consume_timeout, queue, thread_count,
            c = AmqpClient::Channel::Create(hostname, port, username, password, vhost),
            is_running = this->is_running] {

            // derive the msgpack type
            using traits = typename misc::fn_traits<ConsumeAckFn>;
            using arg0_t = typename traits::template arg_t<0>;
            using T = std::remove_const_t<std::remove_reference_t<arg0_t>>;

            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);
            const auto timeout_ms = details::convert_timeout(consume_timeout);

            ctpl::thread_pool pool(thread_count);

            while (is_running->load()) {
                AmqpClient::Envelope::ptr_t env = nullptr;
                const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, timeout_ms);

                // no message => timeout
                if (has_msg && is_running->load()) {
                    pool.push([c, consume_ack_fn, env](const int id) {
                        // wraps a try-catch block to play safe when calling external function
                        const auto ack_res =
                            [&consume_ack_fn, &env]() -> rustfp::Result<easyamqp::ack, std::unique_ptr<std::exception>> {
                                try {
                                    auto value_res = details::unpack<T>(env->Message()->Body());

                                    return std::move(value_res).match(
                                        [&consume_ack_fn](T &&value) {
                                            return rustfp::Ok(consume_ack_fn(std::move(value)));
                                        },
                                        [](const auto &) {
                                            // failed unpacking, auto acknowledge
                                            return rustfp::Ok(ack::ack);
                                        });
                                }
                                catch (const std::exception &e){
                                    return rustfp::Err(std::make_unique<std::exception>(e));
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
        }) {

    }

    inline subscriber::~subscriber() {
        is_running->store(false);

        if (t.joinable()) {
            t.join();
        }
    }

    template <class T>
    auto publish(
        const std::string &queue,
        const T &value,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) noexcept
        -> rustfp::Result<rustfp::unit_t, err_t> {

        try {
            auto c = AmqpClient::Channel::Create(hostname, port, username, password, vhost);
            auto str_res = details::pack(value);

            return std::move(str_res)
                .map([&c, &queue](std::string &&str) {
                    const auto basic_msg = AmqpClient::BasicMessage::Create(str);
                    c->BasicPublish("", queue, basic_msg);
                    return rustfp::Unit;
                });
        }
        catch (const std::exception &e) {
            return rustfp::Err(std::make_unique<std::exception>(e));
        }
    }

    template <class T, class ConsumeFn>
    auto consume_for(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::chrono::milliseconds &consume_timeout,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) noexcept
        -> rustfp::Result<std::result_of_t<ConsumeFn(T &&)>, consume_for_error_t> {

        try {
            auto c = AmqpClient::Channel::Create(hostname, port, username, password, vhost);
            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);

            AmqpClient::Envelope::ptr_t env = nullptr;
            const auto timeout_ms = details::convert_timeout(consume_timeout); 
            const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, timeout_ms);

            if (has_msg) {
                auto value_res = details::unpack<T>(env->Message()->Body());

                return std::move(value_res)
                    .map([&c, &consume_fn, &env](T &&value) {
                        auto consume_opt = consume_fn(std::move(value));

                        if (consume_opt.is_some()) {
                            c->BasicAck(env);
                        }
                        else {
                            c->BasicReject(env, true);
                        }

                        return std::move(consume_opt);
                    })
                    .map_err([](err_t &&e) {
                        return consume_for_error_t{std::move(e)};
                    });
            } else {
                return rustfp::Err(consume_for_error_t{timeout_t{}});
            }
        }
        catch (const std::exception &e) {
            return rustfp::Err(consume_for_error_t(std::make_unique<std::exception>(e)));
        }
    }

    template <class T, class ConsumeFn>
    auto consume(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::string &hostname,
        const int port,
        const std::string &username,
        const std::string &password,
        const std::string &vhost) noexcept
        -> rustfp::Result<std::result_of_t<ConsumeFn(T &&)>, err_t> {

        using opt_t = std::result_of_t<ConsumeFn(T &&)>;
        using res_t = rustfp::Result<opt_t, err_t>;

        auto consume_res_opt = rustfp::cycle(rustfp::Unit)
            | rustfp::find_map([&queue, &consume_fn, &hostname, port,
                &username, &password, &vhost](rustfp::unit_t) -> rustfp::Option<res_t> {
                
                auto res = consume_for<T>(queue, consume_fn, details::DEFAULT_DUR, hostname,
                    port, username, password, vhost);

                return std::move(res).match(
                    [](opt_t &&v) {
                        return rustfp::Some(res_t(rustfp::Ok(std::move(v))));
                    },

                    [](consume_for_error_t &&e) -> rustfp::Option<res_t> {
                        if (e.is_timeout()) {
                            return rustfp::None;
                        }
                        else {
                            return rustfp::Some(res_t(rustfp::Err(e.get_err_unchecked())));
                        }
                    });
            });

        // since it is a cycle + find_map, it is guaranteed to find Some
        assert(consume_res_opt.is_some());

        return std::move(consume_res_opt).unwrap_unchecked();
    }
}
