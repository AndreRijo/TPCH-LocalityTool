# Debian image with go installed and configured at /go
FROM golang

#Sets base directory for remaining commands
WORKDIR /go

# Adding modules and downloading online dependencies
COPY potionDB/go.mod potionDB/
COPY potionDB/go.sum potionDB/
COPY tpch_client/go.mod tpch_client/
COPY tpch_client/go.sum tpch_client/
COPY tpch_data_processor/go.mod tpch_data_processor/
COPY tpch_data_processor/go.sum tpch_data_processor/
RUN cd tpch_data_processor && go mod download

# Adding local dependencies code + program code
COPY potionDB/src/clocksi potionDB/src/clocksi
COPY potionDB/src/tools potionDB/src/tools
COPY potionDB/src/crdt potionDB/src/crdt
COPY potionDB/src/proto potionDB/src/proto
COPY potionDB/src/antidote potionDB/src/antidote
COPY potionDB/src/shared potionDB/src/shared
COPY potionDB/tpch_helper potionDB/tpch_helper
COPY tpch_client/src tpch_client/src
COPY tpch_data_processor/main tpch_data_processor/main
COPY tpch_data_processor/dp tpch_data_processor/dp
COPY tpch_data_processor/dockerstuff tpch_data_processor/
RUN cd tpch_data_processor/main && go build

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
CMD ["bash", "tpch_data_processor/start.sh"]
