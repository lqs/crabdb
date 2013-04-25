CC ?= gcc
OPTIMIZE ?= -g -O2
CFLAGS = -march=native -msse4.2 -Wall -Wextra -Werror -fno-strict-aliasing -std=gnu99 -D_GNU_SOURCE -I. -Islab -Istorage $(OPTIMIZE)
LDFLAGS =  -lz -lmsgpack -lpthread -ldb -ltcc -ldl $(OPTIMIZE)

HEADERS = table.h bucket.h schema.h server.h crabql.h bitfield.h crabql.h log.h binlog.h stats.h geo.h

OBJS = table.o bucket.o schema.o server.o bitfield.o command.o crabql.o log.o binlog.o storage/storage.o crc32.o slab/rwlock.o slab/slab.o storage/bdblib.o utils.o command_multifind.o stats.o geo.o

all: crabdb

main.o: $(HEADERS)
table.o: $(HEADERS)
bucket.o: $(HEADERS)
server.o: $(HEADERS)
command.o: $(HEADERS)

bitfield.function.get.inc: bitfield_gen
	./bitfield_gen

bitfield.function.set.inc: bitfield_gen
	./bitfield_gen

bitfield.struct.inc: bitfield_gen
	./bitfield_gen

bitfield.o: bitfield.function.get.inc bitfield.function.set.inc bitfield.struct.inc

crabql.o: crabql.export.ops.inc

crabql.export.ops.inc: crabql.c
	cpp crabql.c $(CFLAGS) |grep "EXPORT_" |sed -r "s/.*EXPORT_([a-z][0-9a-z_]+)\\(.+/\\1/" |sort |uniq |sed -r "s/(.+)/{\"$$\\1\", EXPORT_\\1},/" >$@

crabdb: $(OBJS) main.o
	$(CC) $^ $(LDFLAGS) -o $@

binlogdump.o: binlogdump.c $(HEADERS)

binlogdump: $(OBJS) binlogdump.o
	$(CC) $(LDFLAGS) $^ -o $@

install:
	cp -f crabdb /usr/bin
	cp -n crabdb.conf /etc/init
	groupadd -f -K GID_MIN=100 -K GID_MAX=999 crabdb
	useradd -d /var/lib/crabdb -K UID_MIN=100 -K UID_MAX=999 -g crabdb crabdb

clean:
	rm -f $(OBJS) main.o crabdb bitfield_gen bitfield.function.get.inc bitfield.function.set.inc bitfield.struct.inc crabql.export.ops.inc

