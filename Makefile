SHELL       = /bin/sh

check:
	echo "PYCHECKER TEST"
	@for i in rushscheduler.py; do				\
	    printf '%-40s' "--- CHECKING $$i";			\
	    if ! pylint --score=no $$i; then exit 1; fi;	\
	    echo 'OK';						\
	done

FORCE:

