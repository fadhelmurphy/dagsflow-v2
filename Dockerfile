# dagsflow-go image: scheduler + webserver + Go toolchain (Go DAG files in the
# DAG folder are compiled on the fly, so the runtime needs `go` and this source).
FROM golang:1.23-bookworm

ENV CGO_ENABLED=0 \
    DAGSFLOW_SDK_DIR=/opt/dagsflow \
    DAGSFLOW_HOME=/opt/dagsflow-home \
    DAGSFLOW_DAGS_FOLDER=/opt/dagsflow-home/dags \
    TZ=Asia/Jakarta

WORKDIR /opt/dagsflow

# dependencies first (cached layer)
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go mod tidy \
 && go build -o /usr/local/bin/dagsflow-go . \
 # warm the Go build cache with the exact flags used for DAG bundles, so the
 # first Go DAG compiles in about a second instead of minutes
 && mkdir -p /tmp/warm && cp dags/features.go /tmp/warm/ \
 && DAGSFLOW_HOME=/tmp/warm-home DAGSFLOW_DAGS_FOLDER=/tmp/warm dagsflow-go dags list >/dev/null \
 && rm -rf /tmp/warm /tmp/warm-home

RUN mkdir -p $DAGSFLOW_HOME/dags $DAGSFLOW_HOME/connections
WORKDIR /opt/dagsflow-home
EXPOSE 8080
ENTRYPOINT ["dagsflow-go"]
CMD ["--help"]
