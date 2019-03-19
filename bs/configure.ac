#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([NRG5-SU-Base Station], [0.1], [furong.yang@lip6.fr luca.demori@lip6.fr])
AC_CONFIG_SRCDIR([src/test/test_fwding.c])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE([NRG5-SU-Base Station], [0.1])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.
AC_CHECK_LIB([pthread], [pthread_create])
AC_CHECK_LIB([wiringPi], [digitalWrite])
AC_PROG_RANLIB

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h netinet/in.h stdint.h stdlib.h string.h sys/ioctl.h sys/socket.h sys/time.h unistd.h signal.h wiringPi.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([gettimeofday memset select socket])

AC_OUTPUT([src/bs_lib/Makefile
           src/test/Makefile
           src/bs_app/Makefile
           Makefile])
