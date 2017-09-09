#include "easyamqp/easyamqp.hpp"

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "rustfp/option.h"

#include <cstddef>
#include <chrono>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// easyamqp
using easyamqp::connection_info;
using easyamqp::consume;
using easyamqp::consume_for;
using easyamqp::publish;
using easyamqp::response;
using easyamqp::subscriber;

// rustfp
using rustfp::None;
using rustfp::Option;
using rustfp::Some;

// std
using std::chrono::milliseconds;
using std::lock_guard;
using std::move;
using std::make_shared;
using std::make_unique;
using std::mutex;
using std::pair;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

class foo {
public:
    template <class BeginIt, class EndIt>
    foo(const BeginIt &begin_it, const EndIt &end_it) :
        values_ptr(make_shared<pair<unique_ptr<mutex>, vector<int>>>(make_unique<mutex>(), vector<int>{begin_it, end_it})) {

    }

    // must be const and have to use mutable with Arc + Mutex
    auto operator()(const vector<int> &values) const -> response {
        lock_guard<mutex> lock(*values_ptr->first);
        auto &this_values = values_ptr->second;

        this_values.insert(
            this_values.cend(),
            values.cbegin(),
            values.cend());

        return response::ack;
    }

    auto get() const -> vector<int> {
        return values_ptr->second;
    }

private:
    mutable shared_ptr<pair<unique_ptr<mutex>, vector<int>>> values_ptr;
};

SCENARIO("SubscriberConnInfoSuccess", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-subscriber-conn-info-success";

    // char array and string will serialize into the same thing
    static constexpr auto TEXT_MSG = "Connection info example";
    string outer_msg;

    const connection_info my_conn_info{"127.0.0.1", 5672, "guest", "guest"};

    subscriber sub(QUEUE_NAME, [&outer_msg](const string &value) {
        outer_msg = value;
        return response::ack;
    }, my_conn_info);

    // publisher sleeps first
    sleep_for(milliseconds(1000));
    publish(QUEUE_NAME, TEXT_MSG);

    // allows consumer to act
    sleep_for(milliseconds(250));

    REQUIRE(TEXT_MSG == outer_msg);
}

SCENARIO("SubscriberConnInfoFail", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-subscriber-conn-info-failure";

    // invalid connection information
    const connection_info my_conn_info{"doesnotexist", 12345, "nosuchusername", "nosuchpassword"};

    REQUIRE_THROWS([&my_conn_info] {
        subscriber sub(QUEUE_NAME, [](string) {
            return response::ack;
        }, my_conn_info);
    }());
}

SCENARIO("SubscriberText", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-subscriber-text";

    // char array and string will serialize into the same thing
    static constexpr auto TEXT_MSG = "This is a text example.";
    string outer_msg;

    subscriber sub(QUEUE_NAME, [&outer_msg](const string &value) {
        outer_msg = value;
        return response::ack;
    });

    // publisher sleeps first
    sleep_for(milliseconds(1000));
    publish(QUEUE_NAME, TEXT_MSG);

    // allows consumer to act
    sleep_for(milliseconds(250));

    REQUIRE(TEXT_MSG == outer_msg);
}

SCENARIO("SubscriberComplexMsgpack", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-subscriber-complex-msgpack";

    static const unordered_map<int, string> NUMBERS {
        { 0, "ZERO" }, { 3, "THREE" }, { 7, "SEVEN" },
    };

    unordered_map<int, string> outer_numbers;

    subscriber sub(QUEUE_NAME, [&outer_numbers](const unordered_map<int, string> &value) {
        outer_numbers = value;
        return response::ack;
    });

    // publisher sleeps first
    sleep_for(milliseconds(2300));
    publish(QUEUE_NAME, NUMBERS);

    // allows consumer to act
    sleep_for(milliseconds(250));

    REQUIRE("SEVEN" == outer_numbers[7]);
    REQUIRE("ZERO" == outer_numbers[0]);
    REQUIRE("THREE" == outer_numbers[3]);
}

SCENARIO("SubscriberFunctor", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-subscriber-functor";
    static const vector<int> NUMBERS {1, 3, 5, 7};
    
    {
        // direct invocation of functor
        // just to test the syntax
        subscriber sub(QUEUE_NAME, foo{NUMBERS.cbegin(), NUMBERS.cend()});
    }

    auto f = foo{NUMBERS.cbegin(), NUMBERS.cend()};
    REQUIRE(4 == f.get().size());
    
    subscriber sub(QUEUE_NAME, f);
    publish(QUEUE_NAME, NUMBERS);

    sleep_for(milliseconds(250));
    REQUIRE(8 == f.get().size());
}

SCENARIO("SubscriberNack", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-subscriber-nack";
    size_t count = 0;

    subscriber sub(QUEUE_NAME, [&count](const string &) {
        ++count;

        return count == 5
            ? response::ack
            : response::rej;
    });

    // publisher
    publish(QUEUE_NAME, "");

    // allows consumer to act
    // this works on the fact that the nack loop re-runs immediately until count is 5
    sleep_for(milliseconds(250));

    REQUIRE(5 == count);
}

SCENARIO("SubscriberMsgpackFail", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-subscriber-msgpack-fail";
    size_t count = 0;

    // failing unpacking on the same queue name
    // will sliently drop the messages
    subscriber sub_fail(QUEUE_NAME, [&count](const string &) {
        ++count;
        return response::ack;
    });

    // fail
    publish(QUEUE_NAME, 3.14);
    publish(QUEUE_NAME, vector<int>{});

    // can unpack
    publish(QUEUE_NAME, "Hello");

    // fail again
    publish(QUEUE_NAME, 777);
    publish(QUEUE_NAME, false);

    // can unpack again
    publish(QUEUE_NAME, "World");

    sleep_for(milliseconds(250));

    REQUIRE(2 == count);
}

SCENARIO("ConsumeForFail", "[EasyAmqp]") {
    const auto res = consume_for("easyamqp-consume-for-fail",
        [](string &&value) { return Some(move(value)); });

    REQUIRE(res.is_err());
    REQUIRE(res.get_err_unchecked().is_timeout());
}

SCENARIO("PublishConsume", "[EasyAmqp]") {
    static constexpr auto QUEUE_NAME = "easyamqp-publish-consume";

    const auto pub_res = publish(QUEUE_NAME, 3.14);
    REQUIRE(pub_res.is_ok());

    // reject acknowledgement then accept
    // note that rejected message will continue to block the consume
    auto will_ack = false;

    // accept acknowledgement
    const auto con_res = consume(QUEUE_NAME,
        [&will_ack](const double value) -> Option<double> {
            if (will_ack) {
                will_ack = false;
                return Some(value);
            }
            else {
                will_ack = true;
                return None;
            }
        });

    REQUIRE(con_res.is_ok());
    REQUIRE(3.14 == con_res.get_unchecked());

    // confirm the queue is empty
    const auto con_for_res = consume_for(QUEUE_NAME,
        [](const double value) { return Some(value); });

    REQUIRE(con_for_res.is_err());
    REQUIRE(con_for_res.get_err_unchecked().is_timeout());
}
