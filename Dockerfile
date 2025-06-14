FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/cmd/places
RUN go build -o places_service

FROM alpine:latest
WORKDIR /root/

COPY --from=builder /app/cmd/places/places_service .

EXPOSE 8080

CMD ["./places_service"]

