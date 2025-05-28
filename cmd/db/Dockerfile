FROM golang:1.24
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /app/cmd/db
RUN go build -o db-server main.go

CMD ["./db-server"]