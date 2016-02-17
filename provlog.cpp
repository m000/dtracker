#include "provlog.H"

/* Array that maps fds to ufds.
 * File descriptors are recycled by the OS, so they are not suitable
 * to be used as taint marks. OTH, ufds monotonically increase, so
 * they are unique through the program execution. We use UINT32 for
 * them, which should be sufficient.
 */
ufdmap_t ufdmap;

/* Set of watched fds - maybe change this to bitset? */
std::set<int> fdset;

/* Counters for stdin/stdout/stderr.
 * TODO: Maybe this should be generalized. I.e. maintain counters for
 * all fds where isatty(fd) returns true.
 */
off_t stdcount[STDFD_MAX];

/* Raw provenance output stream. */
std::ofstream rawProvStream;

/* Current executable name and pid.
 * XXX: Check if this works correctly while following execv().
 */
std::string exename("N/A");
pid_t pid;

/* vim: set noet ts=4 sts=4 sw=4 ai ft=make : */