#pragma once
#include <string>
#include <iostream>
#include <stdexcept>
#include <time.h>
#include <thread>

constexpr bool DISABLE_CERR_ERRORS = true;
constexpr bool ENABLE_LOG_FNAME = true;
constexpr bool ENABLE_LOG_DEBUG_MSG = false;
constexpr bool ENABLE_LOG_INFO_MSG = true;
constexpr bool ENABLE_LOG_ERR_MSG = true; // always keep true

static inline long double time_monotonic() {
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    return (time.tv_sec * 1000000000.0L ) + time.tv_nsec;
}

#define ASS(cond, msg) assert_msg(cond, std::string(__PRETTY_FUNCTION__) + std::string(" assert fail w msg: ") + std::string(msg));
inline void assert_msg(bool cond, const std::string& msg) {
	if (!cond) {
		throw std::logic_error(msg);
	}
}
#define LOG_FNAME if (ENABLE_LOG_FNAME) log_server(" " ,__PRETTY_FUNCTION__)
#define LOG_DEBUG_MSG(...) if (ENABLE_LOG_DEBUG_MSG) log_server("[DEB] :", std::this_thread::get_id(), ": " ,__PRETTY_FUNCTION__, " ", __VA_ARGS__)
#define LOG_INFO_MSG(...) if (ENABLE_LOG_INFO_MSG) log_server("[INFO] :", std::this_thread::get_id(), ": " ,__PRETTY_FUNCTION__, " ", __VA_ARGS__)
#define LOG_ERR_MSG(...) if (ENABLE_LOG_ERR_MSG) log_server("[ERR] :", std::this_thread::get_id(), ": " ,__PRETTY_FUNCTION__, " ", __VA_ARGS__)

//, int(get_server_state())
template <class... T>
inline void log_server(const T&... args) {
    if constexpr (!DISABLE_CERR_ERRORS) {
        (std::cerr << ... << args) << '\n';
    }
}

//std::ostream& operator<<(std::ostream& o, gRPCServiceImpl::ServerState st)
//{
//	o << ((st == 0) ? 'P' : 'B');
//	return o;
//}
