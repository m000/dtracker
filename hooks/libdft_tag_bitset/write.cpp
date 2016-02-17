#include "hooks/hooks.H"

#include <map>
#include <set>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "libdft_api.h"
#include "tagmap.h"
#include "pin.H"

#include "provlog.H"
#include "dtracker.H"
#include "osutils.H"


#include "pin.H"


// #define __DEBUG_SYSCALL_WRITE
#ifdef __DEBUG_SYSCALL_WRITE
static inline std::string __RANGE2STR(const range_map_t & rmap) {
	std::string s;
	for (auto &r : rmap) {
		s += decstr(r.first.first) + ":" + decstr(r.first.second) + 
			"(" + decstr((int)r.second.type) + "," + decstr(r.second.start) +
			"," + decstr(r.second.length) + ") ";
	}
	return s;
}
#endif

/*
 * write(2) handler (taint-sink)
 * 
 * Signature: ssize_t write(int fd, const void *buf, size_t count);
 */
#define DEF_SYSCALL_WRITE
#include "hooks/syscall_args.h"
template<>
void post_write_hook<libdft_tag_bitset>(syscall_ctx_t *ctx) {
	/* ignore write() on not watched fd */
	if (unlikely(fdset.find(_FD) == fdset.end()))
		return;

	/* write() was not successful; optimized branch; errno message may be incorrect */
	if (unlikely(_N_WRITTEN < 0)) {
		LOG("ERROR " _CALL_LOG_STR + " (" + strerror(errno) + ")\n");
		return;
	}

	LOG("OK    " _CALL_LOG_STR + "\n");

	const ufd_t ufd = ufdmap.get(_FD);
	off_t write_begin;

	/* calculate begining of write */
	if (IS_STDFD(_FD)) {
		write_begin = stdcount[_FD];
		stdcount[_FD] += _N_WRITTEN;
	}
	else {
		write_begin = lseek(_FD, 0, SEEK_CUR) - _N_WRITTEN;
		if ( unlikely(write_begin < 0) ){
			LOG("Error on L" + decstr(__LINE__) + " lseek-ing on fd" + decstr(_FD) + ": " + strerror(errno) + "\n");
			return;
		}
	}

	for(ssize_t i=0; i<_N_WRITTEN; i++) { //loop through memory locations
		tag_t tag = tagmap_getb(_BUF+i);

		for(unsigned int j=0; j<tag.size(); j++) {
			if (!tag[j]) continue;
std::cout<<ufd<<std::endl;
			// PROVLOG_WRITE_RANGE(ufd, write_begin, 1, range_info_t::NONE);
		}

// #ifdef __DEBUG_SYSCALL_WRITE
// 		LOG("---------------------- " + std::string((char *)(_BUF+i), 1) + "\n");
// 		LOG("RANGES " + __RANGE2STR(ranges) + "\n");
// 		LOG("RANGES_PREV " + __RANGE2STR(ranges_prev) + "\n");
// #endif


	} //loop memory locations

}
#define UNDEF_SYSCALL_WRITE
#include "hooks/syscall_args.h"

template<>
void post_writev_hook<libdft_tag_bitset>(syscall_ctx_t *ctx) {
	LOG("Writev. Not supported yet.\n");
}

/* vim: set noet ts=4 sts=4 sw=4 ai ft=make : */