/**
 * Type traits to infer other compile-time types or arity of function types.
 * @author Chen Weiguang
*/

#pragma once

#include <cstddef>
#include <tuple>
#include <type_traits>

namespace misc {
    /**
     * Provides extraction of types from functions.
     * Does not work for overloaded functions. Overloaded functions should be std::result_of instead.
     */
    template <class Fn>
    struct fn_traits;

    /**
     * Works for non-class based function type.
     */
    template <class R, class... Args>
    struct fn_traits<R(Args...)> {
        /**
         * Constant time value, number of function parameters.
         */
        static constexpr auto arity = sizeof...(Args);

        /**
         * Alias to the return type.
         */
        using return_t = R;

        /**
         * Helper structure to get the type of individual parameter.
         */
        template <size_t Ind>
        struct arg {
            /**
             * Alias to the nth parameter type of the function.
             */
            using type = typename std::tuple_element<Ind, std::tuple<Args...>>::type;
        };

        /**
         * Helper structure to get the type of individual parameter.
         */
        template <size_t Ind>
        using arg_t = typename arg<Ind>::type;
    };

    /**
     * Works for non-class based function pointer type.
     */
    template <class R, class... Args>
    struct fn_traits<R(*)(Args...)> : public fn_traits<R(Args...)> {};

    /**
     * Works for class based method pointer type.
     */
    template <class R, class C, class... Args>
    struct fn_traits<R(C::*)(Args...)> : public fn_traits<R(Args...)> {};

    /**
     * Works for const class based method pointer type.
     */
    template <class R, class C, class... Args>
    struct fn_traits<R(C::*)(Args...) const> : public fn_traits<R(Args...)> {};

    /**
     * Works for all other function/closure types.
     */
    template <class Fn>
    struct fn_traits : public fn_traits<decltype(&Fn::operator())> {};
}