FROM golang:1.23.4

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod tidy
COPY . /app

RUN go build -o main .

CMD ["./main"]