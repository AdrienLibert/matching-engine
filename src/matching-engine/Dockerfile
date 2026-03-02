FROM golang:1.23.4

WORKDIR /app
COPY src/protocol /app/src/protocol
COPY src/matching-engine /app/src/matching-engine

WORKDIR /app/src/matching-engine

RUN go build -o main .

CMD ["./main"]