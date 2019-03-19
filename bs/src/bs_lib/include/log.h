#ifndef __LOG_H__
#define __LOG_H__

#include "config.h"
#include <stdio.h>

#define LOG_LEVEL(lvl, fmt, ...) fprintf(stdout, "[" lvl "](%s:%s:%d): " fmt, __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#if defined(LOG_DEBUG_ON)
    #define LOG_DEBUG(fmt, ...) LOG_LEVEL("DEBUG", fmt, ##__VA_ARGS__)
#else
    #define LOG_DEBUG(fmt, ...)
#endif


#if defined(LOG_INFO_ON)
    #define LOG_INFO(fmt, ...) LOG_LEVEL("INFO", fmt, ##__VA_ARGS__)
#else
    #define LOG_INFO(fmt, ...)
#endif


#endif
