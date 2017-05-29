#include "gtest/gtest.h"

#include "easyamqp/easyamqp.hpp"

#include "rustfp/option.h"

#include <cstddef>
#include <chrono>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

// easyamqp
using easyamqp::ack;
using easyamqp::consume;
using easyamqp::consume_for;
using easyamqp::publish;
using easyamqp::subscriber;

// rustfp
using rustfp::None;
using rustfp::Option;
using rustfp::Some;

// std
using std::chrono::milliseconds;
using std::move;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using std::unordered_map;

// template <class T>
// auto msgpack_to_str(const T &ser) -> string {
//     stringstream buf;
//     msgpack::pack(buf, ser);
//     return buf.str();
// }

// template <class T>
// auto str_to_msgpack(const string &msg) -> T {
//     T value;
    
//     const auto handle = msgpack::unpack(msg.data(), msg.length());
//     const auto deserialized = handle.get();
//     deserialized.convert(value);

//     return move(value);
// }

TEST(EasyAmqp, DualChannelText) {
    static constexpr auto QUEUE_NAME = "easyamqp-dual-channel-text";
    static const string TEXT_MSG = "This is a text example.";
    string outer_msg;

    subscriber<string> sub(QUEUE_NAME, [&outer_msg](const string &value) {
        outer_msg = value;
        return ack::ack;
    });

    // publisher sleeps first
    sleep_for(milliseconds(1000));
    publish(QUEUE_NAME, TEXT_MSG);

    // allows consumer to act
    sleep_for(milliseconds(250));

    EXPECT_EQ(TEXT_MSG, outer_msg);
}

TEST(EasyAmqp, DualChannelMsgpack) {
    static constexpr auto QUEUE_NAME = "easyamqp-dual-channel-msgpack";

    static const unordered_map<int, string> NUMBERS {
        { 0, "ZERO" }, { 3, "THREE" }, { 7, "SEVEN" },
    };

    unordered_map<int, string> outer_numbers;

    subscriber<unordered_map<int, string>> sub(QUEUE_NAME, [&outer_numbers](const unordered_map<int, string> &value) {
        outer_numbers = value;
        return ack::ack;
    });

    // publisher sleeps first
    sleep_for(milliseconds(2300));
    publish(QUEUE_NAME, NUMBERS);

    // allows consumer to act
    sleep_for(milliseconds(250));

    EXPECT_EQ("SEVEN", outer_numbers[7]);
    EXPECT_EQ("ZERO", outer_numbers[0]);
    EXPECT_EQ("THREE", outer_numbers[3]);
}

TEST(EasyAmqp, DualChannelNack) {
    static constexpr auto QUEUE_NAME = "easyamqp-dual-channel-nack";
    size_t count = 0;

    subscriber<string> sub(QUEUE_NAME, [&count](const string &value) {
        ++count;

        return count == 5
            ? ack::ack
            : ack::rej;
    });

    // publisher
    publish(QUEUE_NAME, "");

    // allows consumer to act
    // this works on the fact that the nack loop re-runs immediately until count is 5
    sleep_for(milliseconds(250));

    EXPECT_EQ(5, count);
}

TEST(EasyAmqp, ConsumeForFail) {
    const auto res = consume_for<string>("easyamqp-consume-for-fail",
        [](string &&value) { return Some(move(value)); });

    EXPECT_TRUE(res.is_err());
    EXPECT_TRUE(res.get_err_unchecked().is_timeout());
}

TEST(EasyAmqp, PublishConsume) {
    static constexpr auto QUEUE_NAME = "easyamqp-publish-consume";

    const auto pub_res = publish(QUEUE_NAME, 3.14);
    EXPECT_TRUE(pub_res.is_ok());

    // reject acknowledgement
    const auto con_res1 = consume<double>(QUEUE_NAME,
        [](const double value) -> Option<string> { return None; });

    EXPECT_TRUE(con_res1.is_ok());
    EXPECT_TRUE(con_res1.get_unchecked().is_none());

    // accept acknowledgement
    const auto con_res2 = consume<double>(QUEUE_NAME,
        [](const double value) { return Some(value); });

    EXPECT_TRUE(con_res2.is_ok());
    EXPECT_TRUE(con_res2.get_unchecked().is_some());
    EXPECT_EQ(3.14, con_res2.get_unchecked().get_unchecked());

    // confirm the queue is empty
    const auto con_for_res = consume_for<double>(QUEUE_NAME,
        [](const double value) { return Some(value); });

    EXPECT_TRUE(con_for_res.is_err());
    EXPECT_TRUE(con_for_res.get_err_unchecked().is_timeout());
}

int main(int argc, char * argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
