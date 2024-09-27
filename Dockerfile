# Debian image with go installed and configured at /go
FROM golang:1.20.4 as base

#Sets base directory for remaining commands
WORKDIR /go

# Adding modules and downloading online dependencies

COPY potionDB/crdt/go.mod potionDB/crdt/go.sum potionDB/crdt/
COPY potionDB/shared/go.mod potionDB/shared/
COPY tpch_data_processor/go.mod tpch_data_processor/go.sum tpch_data_processor/
COPY tpch_locality_tool/go.mod tpch_locality_tool/go.sum tpch_locality_tool/
RUN cd tpch_locality_tool && go mod download

# Adding local dependencies code + program code
COPY potionDB/crdt potionDB/crdt
COPY potionDB/shared potionDB/shared
COPY tpch_data_processor/tpch tpch_data_processor/tpch
COPY tpch_locality_tool/main tpch_locality_tool/main
COPY tpch_locality_tool/dp tpch_locality_tool/dp
COPY tpch_locality_tool/dockerstuff tpch_locality_tool/
RUN cd tpch_locality_tool/main && go build

#Arguments
ENV DATA_LOC "/go/data/" \
SF -1 \
ORDER_LOCALITY -1 \
ITEM_LOCALITY -1 \
ONE_REM_RATE -1 \
TWO_REM_RATE -1 \
TWO_DIFF_REG_REM_RATE -1 \
N_UPD_FILES -1

# Run
CMD ["bash", "tpch_locality_tool/start.sh"]
