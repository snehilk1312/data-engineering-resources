FROM golang:1.16
EXPOSE 8080
WORKDIR /usr/src/app
COPY . .
RUN unset GOPATH
RUN go build


ENV REQUEST_ORIGIN="http://localhost:5000"
ENV PORT=8080
CMD ["./server"]
