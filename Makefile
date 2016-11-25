MYPATH=$(shell pwd)
PROTOC=protoc
RM=rm
BIN=server/bfserver
TOOL=tools/bfserver_tool
SRCS=server/main.go
TOOL_SRCS=tools/main.go
GOBUILD=go generate && go build -v && go test -v
DEPENDS=$(wildcard *.go g/*.go bloom/*.go)
PBOBJS=$(patsubst %.proto,%.pb.go,$(wildcard bloomiface/*.proto))
OUTPUT=${MYPATH}/output
CONFDIR=${MYPATH}/conf
BINDIR=${MYPATH}/bin
RUNDIR=${MYPATH}/run

.PHONY: all

all: ${BIN} ${TOOL}
	rm -rf ${OUTPUT}
	mkdir -p ${OUTPUT}/bin
	mkdir -p ${OUTPUT}/conf
	mkdir -p ${OUTPUT}/run
	mkdir -p ${OUTPUT}/tools
	cp ${BIN} ${OUTPUT}/bin
	cp ${TOOL} ${OUTPUT}/tools
	cp ${BINDIR}/* ${OUTPUT}/bin
	cp ${CONFDIR}/* ${OUTPUT}/conf

clean:
	${RM} -f $(PBOBJS) ${BIN}

test:
	go test -v 

${TOOL}: ${PBOBJS} ${TOOL_SRCS} ${DEPENDS}
	go build -o ${TOOL} ${TOOL_SRCS}

${BIN}: ${PBOBJS} ${SRCS} ${DEPENDS}
	go build -o ${BIN} ${SRCS}

%.pb.go: %.proto
	${PROTOC} --go_out=plugins=grpc:bloomiface --php_out=/home/work/banews-server/library/pb/ $^  -I bloomiface/ -I=${GOPATH}/src
