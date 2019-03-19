#ifndef __UTIL_H__
#define __UTIL_H__

#include <time.h>
#include <sys/time.h>
#include <stdint.h>

static void make_timeout(struct timespec *ts, int millisec) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint32_t sec = millisec / 1000;
    uint32_t nsec = (millisec % 1000) * 1000000;
    ts->tv_sec = tv.tv_sec + sec;
    ts->tv_nsec = tv.tv_usec * 1000 + nsec;
    ts->tv_sec += (ts->tv_nsec / 1000000000);
    ts->tv_nsec = (ts->tv_nsec % 1000000000);
}

#endif
