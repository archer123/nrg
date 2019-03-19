noinst_PROGRAMS=test_wifi test_arp test_fwd test_nbr test_nso
test_wifi_SOURCES=test_wifi.c
test_arp_SOURCES=test_arp.c
test_fwd_SOURCES=test_fwding.c
test_nbr_SOURCES=test_nbr.c
test_nso_SOURCES=test_nso.c

test_wifi_LDADD=$(top_srcdir)/src/bs_lib/libbs.a
test_arp_LDADD=$(top_srcdir)/src/bs_lib/libbs.a
test_fwd_LDADD=$(top_srcdir)/src/bs_lib/libbs.a
test_nbr_LDADD=$(top_srcdir)/src/bs_lib/libbs.a
test_nso_LDADD=$(top_srcdir)/src/bs_lib/libbs.a

INCLUDES=-I$(top_srcdir)/src/bs_lib/include
