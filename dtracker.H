#ifndef DTRACKER_H
#define DTRACKER_H

/**** generic macros and definitions ******************************/
#define BOOL(x) ((x) ? 1 : 0)

/* min/max macros for general use */
#if !defined(MIN)
#define MIN(a,b) (((a)<(b))?(a):(b))
#endif
#if !defined(MAX)
#define MAX(a,b) (((a)>(b))?(a):(b))
#endif

/* compiler directives for branch prediction */
#if !defined(likely)
#define likely(x)	__builtin_expect((x), 1)
#endif
#if !defined(unlikely)
#define unlikely(x)	__builtin_expect((x), 0)
#endif

/**** debugging macros and inlines ********************************/
#ifdef NO_PINTOOL_LOG
#ifdef LOG
#undef LOG
#endif
#define LOG(args...) do{} while(0)
#endif

#endif

/* vim: set noet ts=4 sts=4 sw=4 ai : */
