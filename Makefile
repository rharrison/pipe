CC=gcc

OBJS=pipe.c pipe_test.c 
NAME=pipe

CFLAGS=-Wall -Wextra -Wpointer-arith -fstrict-aliasing -std=c99 -DFORTIFY_SOURCE=2 -pipe -pedantic #-Werror
D_CFLAGS=-DDEBUG -g -O0
R_CFLAGS=-DNDEBUG -O3 -funroll-loops #-pg #-flto

target = $(shell sh -c '$(CC) -v 2>&1 | grep "Target:"')

ifeq (,$(findstring mingw,$(target)))
	CFLAGS += -pthread
endif

all: pipe_test 

pipe_test: $(OBJS) pipe_test.c
	$(CC) $(CFLAGS)  $(D_CFLAGS) -o pipe_test $(OBJS)

pipe.h:

pipe.c: pipe.h

pipe_test.c: pipe.h 

.PHONY : clean 

clean:
	rm -f pipe_test
