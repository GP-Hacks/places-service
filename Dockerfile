FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY places/go.mod places/go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/places/cmd/places
RUN go build -o places_service

FROM alpine:latest
WORKDIR /root/

COPY --from=builder /app/places/cmd/places/places_service .

EXPOSE 8080

CMD ["./places_service"]
