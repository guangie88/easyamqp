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
#include "rustfp/once.h"
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

        /** To get the first argument type of a non-overloaded function. */
        template <class Fn>
        using arg0_t = std::remove_cv_t<std::remove_reference_t<
            typename misc::fn_traits<
                std::remove_cv_t<std::remove_reference_t<Fn>>>::template arg_t<0>>>;

        /** To get the result type of a non-overloaded function. */
        template <class Fn>
        using result_t = typename misc::fn_traits<
            std::remove_cv_t<std::remove_reference_t<Fn>>>::return_t;
    }

    /**
     * Groups all the fields required for connection into a structure.
     */
    struct connection_info {
        /**
         * Constructor to initialize all the fields.
         * @param hostname AMQP server hostname.
         * @param port AMQP server port.
         * @param username username for connection login.
         * @param password password for connection login.
         * @param vhost virtual host of the AMQP exchange.
         */
        connection_info(
            const std::string &hostname = details::DEFAULT_HOSTNAME,
            const int port = details::DEFAULT_PORT,
            const std::string &username = details::DEFAULT_USERNAME, 
            const std::string &password = details::DEFAULT_PASSWORD,
            const std::string &vhost = details::DEFAULT_VHOST) :

            hostname(hostname),
            port(port),
            username(username),
            password(password),
            vhost(vhost) {

        }

        /** Hostname of the AMQP server. */
        std::string hostname;

        /** Port number of the AMQP server. */
        int port;

        /** Username for connection login. */
        std::string username;

        /** Password for connection login. */
        std::string password;

        /** Virtual host of the AMQP exchange. */
        std::string vhost;
    };
    
    namespace details {
        /** Default connection information */
        static const auto DEFAULT_CONN = connection_info{};
    }

    /** Acknowledgement enumeration. */
    enum class response {
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
         * when available in queue, and returns response.
         * @param conn_info structure containing fields required for AMQP connection.
         * @param consume_timeout consume wait timeout duration before
         * performing another blocking wait.
         * @param thread_count number of threads permitted to run at the same time
         * to handle the received messages.
         */
        template <class ConsumeAckFn>
        subscriber(
            const std::string &queue,
            const ConsumeAckFn &consume_ack_fn,
            const connection_info &conn_info = details::DEFAULT_CONN,
            const std::chrono::milliseconds &consume_timeout = details::DEFAULT_DUR,
            const uint32_t thread_count = 1);

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
     * @param conn_info structure containing fields required for AMQP connection.
     * @return Ok<unit_t> describes no error,
     * while Err<exception> holds the thrown exception while publishing.
     */
    template <class T>
    auto publish(
        const std::string &queue,
        const T &value,
        const connection_info &conn_info = details::DEFAULT_CONN) noexcept
        -> rustfp::Result<rustfp::unit_t, err_t>;

    /**
     * Consumes any given message in the queue, up to the timeout duration.
     *
     * The created queue will be durable, non-exclusive and does not
     * get auto-deleted.
     * @param queue queue name to create
     * @param consume_fn consumer function, takes in msgpack serializable value
     * and produces any Option.
     * Returning Some(_) acknowledges the message, while None rejects and requeues the message.
     * @param consume_timeout consumer wait timeout duration.
     * @param conn_info structure containing fields required for AMQP connection.
     * @return Ok<_> on success,
     * while Err<consume_for_error_t> holds the variant error value.
     */
    template <class ConsumeFn>
    auto consume_for(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::chrono::milliseconds &consume_timeout = details::DEFAULT_DUR,
        const connection_info &conn_info = details::DEFAULT_CONN) noexcept
        -> rustfp::Result<typename details::result_t<ConsumeFn>::some_t, consume_for_error_t>;

    /**
     * Consumes any given message in the queue, waiting until the message arrives.
     *
     * The created queue will be durable, non-exclusive and does not
     * get auto-deleted.
     * @param queue queue name to create
     * @param consume_fn consumer function, takes in msgpack serializable value
     * and produces any Option.
     * Returning Some(_) acknowledges the message, while None rejects and requeues the message.
     * @param consume_timeout consumer wait timeout duration.
     * @param conn_info structure containing fields required for AMQP connection.
     * @return Ok<_> on success,
     * while Err<err_t> holds the exception error value.
     */
    template <class ConsumeFn>
    auto consume(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const connection_info &conn_info = details::DEFAULT_CONN) noexcept
        -> rustfp::Result<typename details::result_t<ConsumeFn>::some_t, err_t>;

    // implementation section

    namespace details {
        inline auto convert_timeout(const std::chrono::milliseconds &timeout) -> int32_t {
            return timeout.count() >= std::numeric_limits<int32_t>::max()
                ? std::numeric_limits<int32_t>::max()
                : static_cast<int32_t>(timeout.count());
        }

        template <class T>
        auto pack(const T &value) noexcept
            -> rustfp::Result<std::string, std::unique_ptr<std::exception>> {

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
        auto unpack(const std::string &str) noexcept
            -> rustfp::Result<T, std::unique_ptr<std::exception>> {

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
        const connection_info &conn_info,
        const std::chrono::milliseconds &consume_timeout,
        const uint32_t thread_count) :

        // to indicate to stop
        is_running(std::make_shared<std::atomic_bool>(true)),

        // looping thread
        t([consume_ack_fn, consume_timeout, queue, thread_count,
            c = AmqpClient::Channel::Create(
                conn_info.hostname,
                conn_info.port, 
                conn_info.username,
                conn_info.password,
                conn_info.vhost),
            is_running = this->is_running] {

            // derive the msgpack type
            // MSVC has bug, so need to use decltype since ConsumeAckFn is
            // apparently 'not captured'
#ifdef _WIN32
            using ConsumeAckFn = decltype(consume_ack_fn);
#endif

            using T = details::arg0_t<ConsumeAckFn>;

            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);
            const auto timeout_ms = details::convert_timeout(consume_timeout);

            ctpl::thread_pool pool(thread_count);

            while (is_running->load()) {
                AmqpClient::Envelope::ptr_t env = nullptr;
                const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, timeout_ms);

                // no message => timeout
                if (has_msg && is_running->load()) {
                    pool.push([c, consume_ack_fn, env](const int) {
                        // wraps a try-catch block to play safe when calling external function
                        const auto rsp_res =
                            [&consume_ack_fn, &env]()
                                -> rustfp::Result<
                                    easyamqp::response, std::unique_ptr<std::exception>> {

                                try {
                                    auto value_res = details::unpack<T>(env->Message()->Body());

                                    return std::move(value_res).match(
                                        [&consume_ack_fn](T &&value) {
                                            return rustfp::Ok(consume_ack_fn(std::move(value)));
                                        },
                                        [](const auto &) {
                                            // failed unpacking, auto acknowledge
                                            return rustfp::Ok(response::ack);
                                        });
                                }
                                catch (const std::exception &e){
                                    return rustfp::Err(std::make_unique<std::exception>(e));
                                }
                            }();

                        rsp_res.match(
                            [&c, &env](const auto &rsp) {
                                if (rsp == response::ack) {
                                    c->BasicAck(env);
                                }
                                else {
                                    c->BasicReject(env, true);
                                }
                            },

                            [&c, &env](const auto &) {
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
        const connection_info &conn_info) noexcept
        -> rustfp::Result<rustfp::unit_t, err_t> {

        try {
            auto c = AmqpClient::Channel::Create(
                conn_info.hostname,
                conn_info.port,
                conn_info.username,
                conn_info.password,
                conn_info.vhost);

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

    template <class ConsumeFn>
    auto consume_for(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const std::chrono::milliseconds &consume_timeout,
        const connection_info &conn_info) noexcept
        -> rustfp::Result<typename details::result_t<ConsumeFn>::some_t, consume_for_error_t> {

        using T = details::arg0_t<ConsumeFn>;
        using some_t = typename details::result_t<ConsumeFn>::some_t;
        using ret_t = rustfp::Result<some_t, consume_for_error_t>;

        try {
            auto c = AmqpClient::Channel::Create(
                conn_info.hostname,
                conn_info.port,
                conn_info.username,
                conn_info.password,
                conn_info.vhost);

            c->DeclareQueue(queue, false, true, false, false);
            const auto consumer_tag = c->BasicConsume(queue, "", true, false);

            AmqpClient::Envelope::ptr_t env = nullptr;
            const auto timeout_ms = details::convert_timeout(consume_timeout); 

            auto consume_res_opt = rustfp::once(rustfp::Unit)
                | rustfp::cycle()
                | rustfp::find_map([
                    &c, &consume_fn, &consumer_tag, &env, &timeout_ms](rustfp::unit_t) {

                    const auto has_msg = c->BasicConsumeMessage(consumer_tag, env, timeout_ms);

                    if (has_msg) {
                        auto value_res = details::unpack<T>(env->Message()->Body());

                        auto res = std::move(value_res).match(
                            [&c, &consume_fn, &env](T &&value) -> rustfp::Option<ret_t> {
                                auto consume_opt = consume_fn(std::move(value));

                                if (consume_opt.is_some()) {
                                    c->BasicAck(env);

                                    return rustfp::Some(
                                        ret_t(rustfp::Ok(
                                            std::move(consume_opt).unwrap_unchecked())));
                                }
                                else {
                                    c->BasicReject(env, true);
                                    return rustfp::None;
                                }
                            },
                            [](err_t &&e) {
                                return rustfp::Some(
                                    ret_t(rustfp::Err(consume_for_error_t{std::move(e)})));
                            });

                        return std::move(res);
                    } else {
                        return rustfp::Some(
                            ret_t(rustfp::Err(consume_for_error_t{timeout_t{}})));
                    }
                });

            // since it is a cycle + find_map, it is guaranteed to find Some
            assert(consume_res_opt.is_some());

            return std::move(consume_res_opt).unwrap_unchecked();
        }
        catch (const std::exception &e) {
            return rustfp::Err(consume_for_error_t(std::make_unique<std::exception>(e)));
        }
    }

    template <class ConsumeFn>
    auto consume(
        const std::string &queue,
        const ConsumeFn &consume_fn,
        const connection_info &conn_info) noexcept
        -> rustfp::Result<typename details::result_t<ConsumeFn>::some_t, err_t> {

        using opt_t = details::result_t<ConsumeFn>;
        using some_t = typename opt_t::some_t;
        using ret_t = rustfp::Result<some_t, err_t>;

        auto consume_res_opt = rustfp::once(rustfp::Unit)
            | rustfp::cycle()
            | rustfp::find_map([&queue, &consume_fn, &conn_info](rustfp::unit_t)
                -> rustfp::Option<ret_t> {
                
                auto res = consume_for(queue, consume_fn, details::DEFAULT_DUR, conn_info);

                auto opt_res = std::move(res).match(
                    [](some_t &&v) {
                        return rustfp::Some(ret_t(rustfp::Ok(std::move(v))));
                    },

                    [](consume_for_error_t &&e) -> rustfp::Option<ret_t> {
                        if (e.is_timeout()) {
                            return rustfp::None;
                        }
                        else {
                            return rustfp::Some(ret_t(rustfp::Err(e.get_err_unchecked())));
                        }
                    });

                return std::move(opt_res);
            });

        // since it is a cycle + find_map, it is guaranteed to find Some
        assert(consume_res_opt.is_some());

        return std::move(consume_res_opt).unwrap_unchecked();
    }
}
