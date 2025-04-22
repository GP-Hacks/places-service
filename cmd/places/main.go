package main

import (
	"context"
	"github.com/GP-Hacks/kdt2024-commons/prettylogger"
	"github.com/GP-Hacks/kdt2024-places/config"
	"github.com/GP-Hacks/kdt2024-places/internal/grpc-server/handler"
	"github.com/GP-Hacks/kdt2024-places/internal/storage"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"log/slog"
	"net"
)

func main() {
	cfg := config.MustLoad()
	log := prettylogger.SetupLogger(cfg.Env)
	log.Info("Configuration and logger initialized", slog.String("environment", cfg.Env))
	log.Info("Logger initialized")

	grpcServer := grpc.NewServer()

	log.Info("Starting TCP listener", slog.String("address", cfg.Address))
	l, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		logCriticalError(log, "Failed to start listener for PlacesService", err, cfg.Address)
		return
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Error("Failed to close TCP listener", slog.String("error", err.Error()))
		}
	}()
	log.Info("TCP listener started successfully", slog.String("address", cfg.Address))

	storage, err := setupPostgreSQL(cfg, log)
	if err != nil {
		return
	}

	conn, ch, err := setupRabbitMQ(cfg, log)
	if err != nil {
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error("Failed to close RabbitMQ connection", slog.String("error", err.Error()))
		}
		if err := ch.Close(); err != nil {
			log.Error("Failed to close RabbitMQ channel", slog.String("error", err.Error()))
		}
	}()

	if err := declareQueues(cfg, ch, log); err != nil {
		return
	}

	handler.NewGRPCHandler(cfg, grpcServer, storage, log, ch)
	if err := grpcServer.Serve(l); err != nil {
		log.Error("Error serving gRPC server for PlacesService", slog.String("address", cfg.Address), slog.String("error", err.Error()))
	}
}

func setupPostgreSQL(cfg *config.Config, log *slog.Logger) (*storage.PostgresStorage, error) {
	log.Info("Connecting to Postgres", slog.String("address", cfg.PostgresAddress))
	storage, err := storage.NewPostgresStorage(cfg.PostgresAddress + "?sslmode=disable")
	if err != nil {
		logCriticalError(log, "Failed to connect to Postgres", err, cfg.PostgresAddress)
		return nil, err
	}
	log.Info("Postgres connection established")

	if err := storage.CreateTables(context.Background()); err != nil {
		return nil, err
	}

	log.Info("Fetching and storing data")
	if err := storage.FetchAndStoreData(context.Background()); err != nil {
		log.Error("Failed to fetch and store data", slog.String("error", err.Error()))
		return nil, err
	}
	log.Info("Data fetching and storing completed successfully")
	return storage, nil
}

func setupRabbitMQ(cfg *config.Config, log *slog.Logger) (*amqp.Connection, *amqp.Channel, error) {
	log.Info("Connecting to RabbitMQ", slog.String("address", cfg.RabbitMQAddress))
	conn, err := amqp.Dial(cfg.RabbitMQAddress)
	if err != nil {
		logCriticalError(log, "Failed to connect to RabbitMQ", err, cfg.RabbitMQAddress)
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		logCriticalError(log, "Failed to open a channel in RabbitMQ", err, cfg.RabbitMQAddress)
		return nil, nil, err
	}

	log.Info("RabbitMQ connection and channel established successfully")
	return conn, ch, nil
}

func declareQueues(cfg *config.Config, ch *amqp.Channel, log *slog.Logger) error {
	log.Info("Declaring necessary queues in RabbitMQ")

	queues := []string{cfg.QueuePurchases, cfg.QueueNotifications}
	for _, queueName := range queues {
		if _, err := ch.QueueDeclare(
			queueName,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			log.Error("Failed to declare a queue", slog.String("queue_name", queueName), slog.String("error", err.Error()))
			return err
		}
		log.Info("Queue declared successfully", slog.String("queue_name", queueName))
	}

	return nil
}

func logCriticalError(log *slog.Logger, message string, err error, context string) {
	log.Error(message, slog.String("error", err.Error()), slog.String("context", context))
}
