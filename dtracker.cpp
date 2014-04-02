#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include <set>
#include <iostream>
#include <fstream>

#include "syscall_desc.h"
#include "tagmap.h"
#include "pin.H"
#include "osutils.H"

#include "hooks/hooks.H"
#include "dtracker.H"

// #define DTRACKER_DEBUG
#include "dtracker_debug.H" 

/* Syscall descriptors, defined in libdft. */
extern syscall_desc_t syscall_desc[SYSCALL_MAX];

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

/* Current executable name.
 * TODO: Check if this works correctly while following execv().
 */
std::string exename("N/A");

/* Pin knob for setting the raw prov output file */
static KNOB<string> ProvRawKnob(KNOB_MODE_WRITEONCE, "pintool", "o",
	"rawprov.out", "The output file for raw prov data"
);

/* Pin knobs for tracking stdin/stdout/stderr */
static KNOB<string> TrackStdin(KNOB_MODE_WRITEONCE, "pintool", "stdin",
	"0", "Taint data originating from stdin."
);
static KNOB<string> TrackStdout(KNOB_MODE_WRITEONCE, "pintool", "stdout",
	"1", "Log the taint tag data for stdout."
);
static KNOB<string> TrackStderr(KNOB_MODE_WRITEONCE, "pintool", "stderr",
	"0", "Log the taint tag data for stderr."
);



/*
 * Called when a new image is loaded.
 * Currently only acts when the main executable is loaded to set exename global.
 */
static void ImageLoad(IMG img, VOID * v) {
	// TODO: check if this works correctly when execv() is used.
    if (IMG_IsMainExecutable(img)) {
		exename = path_resolve(IMG_Name(img).c_str());
		PROVLOG_EXEC(exename);

		// Add stdin/stdout/stderr to watched file descriptors.
		// This should take place while loading the image in order to have 
		// exename available.
		if ( atoi(TrackStdin.Value().c_str()) ) {
			ufd_t ufd = ufdmap.get(STDIN_FILENO);
			std::string fdn = fdname(STDIN_FILENO);
			fdset.insert(STDIN_FILENO);
			LOG( "Watching fd" + decstr(STDIN_FILENO) + " (" + fdn + ").\n");
			PROVLOG_OPEN(ufd, fdn, fcntl(STDIN_FILENO, F_GETFL), 0);
		}
		if ( atoi(TrackStdout.Value().c_str()) ) {
			ufd_t ufd = ufdmap.get(STDOUT_FILENO);
			std::string fdn = fdname(STDOUT_FILENO);
			fdset.insert(STDOUT_FILENO);
			LOG( "Watching fd" + decstr(STDOUT_FILENO) + " (" + fdn + ").\n");
			PROVLOG_OPEN(ufd, fdn, fcntl(STDOUT_FILENO, F_GETFL), 0);
		}	
		if ( atoi(TrackStderr.Value().c_str()) ) {
			ufd_t ufd = ufdmap.get(STDERR_FILENO);
			std::string fdn = fdname(STDERR_FILENO);
			fdset.insert(STDERR_FILENO);
			LOG( "Watching fd" + decstr(STDERR_FILENO) + " (" + fdn + ").\n");
			PROVLOG_OPEN(ufd, fdn, fcntl(STDERR_FILENO, F_GETFL), 0);
		}
	}
}

/*
 * Called before exit.
 * Handles any fd's that haven't been closed.
 */
static void OnExit(INT32, void *) {
	/* Generate close log entries for remaining ufds.
	 * Don't you love the c++11 loop syntax? 
	 */
	for ( auto &fd : fdset ) {
		ufd_t ufd = ufdmap.get(fd);
		ufdmap.del(fd);
		PROVLOG_CLOSE(ufd);
	}
}


/* 
 * Tool used for verifying that libdft propagates taint correctly.
 */
int main(int argc, char **argv) {
	/* initialize symbol processing */
	PIN_InitSymbols();

	if (unlikely(PIN_Init(argc, argv)))
		goto err;

	IMG_AddInstrumentFunction(ImageLoad, 0);
	PIN_AddFiniFunction(OnExit, 0);

#ifdef DTRACKER_DEBUG
	INS_AddInstrumentFunction(CheckMagicValue, 0);
#endif
	
	LOG("Initializing libdft.\n");
	if (unlikely(libdft_init() != 0))
		goto err;

	// reset counters
	bzero(stdcount, sizeof(stdcount));

	// Open raw prov file.
	// This file is to be post-processed to get the data in a proper format.
    rawProvStream.open(ProvRawKnob.Value().c_str());


	/*
	 * Install taint sources and sinks.
	 * syscall_set_{pre, post}() set the callbacks in the libdft
	 * syscall description struct.
	 * These callbacks are respectively invoked through
	 * sysenter_save() and sysexit_save() function of libdft.
	 * In turn, these libdft functions are hooked to run before/after
	 * every syscall using PIN_AddSyscall{Entry, Exit}Function().
	 */

	/* dtracker_openclose.cpp: open(2), creat(2), close(2) */
	(void)syscall_set_pre(&syscall_desc[__NR_open], pre_open_hook);
	(void)syscall_set_pre(&syscall_desc[__NR_creat], pre_open_hook);
	(void)syscall_set_post(&syscall_desc[__NR_open], post_open_hook);
	(void)syscall_set_post(&syscall_desc[__NR_creat], post_open_hook);
	(void)syscall_set_post(&syscall_desc[__NR_close], post_close_hook);

	/* dtracker_read.cpp: read(2), readv(2) */
	(void)syscall_set_post(&syscall_desc[__NR_read], post_read_hook);
	(void)syscall_set_post(&syscall_desc[__NR_readv], post_readv_hook);

	/* dtracker_write.cpp: write(2), writev(2) */
	(void)syscall_set_post(&syscall_desc[__NR_write], post_write_hook);
	(void)syscall_set_post(&syscall_desc[__NR_writev], post_writev_hook);

	/* dtracker_mmap.cpp: mmap2(2), munmap(2) */
	(void)syscall_set_post(&syscall_desc[__NR_mmap2], post_mmap2_hook);
	(void)syscall_set_post(&syscall_desc[__NR_munmap], post_munmap_hook);


	/* start the program and return something to make the compiler happy */
	LOG("Starting program.\n");
	PIN_StartProgram();	
	return EXIT_SUCCESS;

err:
	/* error handling */

	/* detach from the process */
	libdft_die();

	/* return */
	return EXIT_FAILURE;
}

