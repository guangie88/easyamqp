#include "gtest/gtest.h"

#include "easyamqp/easyamqp.hpp"

#include "msgpack.hpp"

#include <cstddef>
#include <chrono>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

// easyamqp
using easyamqp::ack;
using easyamqp::dual_channel;

// std
using std::chrono::milliseconds;
using std::string;
using std::stringstream;
using std::this_thread::sleep_for;
using std::unordered_map;

static constexpr auto QUEUE_NAME = "c++-queue";

TEST(Main, Text)
{
    static constexpr auto TEXT_MSG = "This is a text example.";
    string outer_msg;

    dual_channel dc(QUEUE_NAME, [&outer_msg](const string &msg)
    {
        outer_msg = msg;
        return ack::ack;
    });

    // publisher sleeps first
    sleep_for(milliseconds(1000));
    dc.publish(TEXT_MSG);

    // allows consumer to act
    sleep_for(milliseconds(250));

    EXPECT_EQ(TEXT_MSG, outer_msg);
}

TEST(Main, Msgpack)
{
    static const unordered_map<int, string> NUMBERS
    {
        { 0, "ZERO" }, { 3, "THREE" }, { 7, "SEVEN" },
    };

    unordered_map<int, string> outer_numbers;

    dual_channel dc(QUEUE_NAME, [&outer_numbers](const string &msg)
    {
        const auto handle = msgpack::unpack(msg.data(), msg.length());
        const auto deserialized = handle.get();
        deserialized.convert(outer_numbers);

        return ack::ack;
    });

    // publisher sleeps first
    sleep_for(milliseconds(2300));
    stringstream buf;
    msgpack::pack(buf, NUMBERS);
    dc.publish(buf.str());

    // allows consumer to act
    sleep_for(milliseconds(250));

    EXPECT_EQ("SEVEN", outer_numbers[7]);
    EXPECT_EQ("ZERO", outer_numbers[0]);
    EXPECT_EQ("THREE", outer_numbers[3]);
}

TEST(Main, Nack)
{
    size_t count = 0;

    dual_channel dc(QUEUE_NAME, [&count](const string &msg)
    {
        ++count;

        return count == 5
            ? ack::ack
            : ack::rej;
    });

    // publisher
    dc.publish("");

    // allows consumer to act
    // this works on the fact that the nack loop re-runs immediately until count is 5
    sleep_for(milliseconds(250));

    EXPECT_EQ(5, count);
}

int main(int argc, char * argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}