FROM golang:1.23.4

WORKDIR /app
COPY src/protocol /app/src/protocol
COPY src/traderpool /app/src/traderpool

WORKDIR /app/src/traderpool

RUN go build -o main .

CMD ["./main"]