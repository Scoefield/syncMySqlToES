FROM golang:1.14
WORKDIR /search
COPY . /search
ENV GOPROXY=https://goproxy.cn,direct
ENV ACTIVE=prd
ENV GIN_MODE=release
RUN export GO111MODULE=on
RUN  go build -o search ./main/search.go
