MYPATH=$(shell pwd)
PROTOC=protoc
RM=rm
BIN=bfserver
GOBUILD=go generate && go build -v && go test -v
SRCS=$(wildcard *.go g/*.go bloom/*.go)
PBOBJS=$(patsubst %.proto,%.pb.go,$(wildcard iface/*.proto))
OUTPUT=${MYPATH}/output
CONFDIR=${MYPATH}/conf
BINDIR=${MYPATH}/bin
RUNDIR=${MYPATH}/run

.PHONY: all

all: ${BIN}
	rm -rf ${OUTPUT}
	mkdir -p ${OUTPUT}/bin
	mkdir -p ${OUTPUT}/conf
	mkdir -p ${OUTPUT}/run
	cp ${BIN} ${OUTPUT}/bin
	cp ${BINDIR}/* ${OUTPUT}/bin
	cp ${CONFDIR}/* ${OUTPUT}/conf

clean:
	${RM} -f $(PBOBJS) ${BIN}


test:
	go test -v 

${BIN}: ${PBOBJS} ${SRCS}
	go generate && go build -v -o $@


%.pb.go: %.proto
	${PROTOC} --go_out=plugins=grpc:iface --php_out=/home/work/banews-server/library/pb/ $^  -I iface/
