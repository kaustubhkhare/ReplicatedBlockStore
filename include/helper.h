#include <string>
#include <stdexcept>

constexpr bool DISABLE_CERR_ERRORS = false;
constexpr bool ENABLE_LOG_FNAME = true;
constexpr bool ENABLE_LOG_DEBUG_MSG = true;
constexpr bool ENABLE_LOG_INFO_MSG = true;
constexpr bool ENABLE_LOG_ERR_MSG = true; // always keep true

#define ASS(cond, msg) assert_msg(cond, std::string(__PRETTY_FUNCTION__) + std::string(" assert fail w msg: ") + std::string(msg));
void assert_msg(bool cond, const std::string& msg) {
	if (!cond) {
		throw std::logic_error(msg);
	}
}
#define LOG_FNAME if (ENABLE_LOG_FNAME) log_server(" " ,__PRETTY_FUNCTION__)
#define LOG_DEBUG_MSG(...) if (ENABLE_LOG_DEBUG_MSG) log_server("[DEB] ", " " ,__PRETTY_FUNCTION__, " ", __VA_ARGS__)
#define LOG_INFO_MSG(...) if (ENABLE_LOG_INFO_MSG) log_server("[INFO] ", " " ,__PRETTY_FUNCTION__, " ", __VA_ARGS__)
#define LOG_ERR_MSG(...) if (ENABLE_LOG_ERR_MSG) log_server("[ERR] ", " " ,__PRETTY_FUNCTION__, " ", __VA_ARGS__)

//, int(get_server_state())
template <class... T>
inline void log_server(const T&... args) {
    if constexpr (!DISABLE_CERR_ERRORS) {
        (std::cerr << ... << args) << '\n';
        std::cerr.flush();
    }
}

//std::ostream& operator<<(std::ostream& o, gRPCServiceImpl::ServerState st)
//{
//	o << ((st == 0) ? 'P' : 'B');
//	return o;
//}
