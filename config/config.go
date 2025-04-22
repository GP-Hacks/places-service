package config

import "os"

type Config struct {
	Env                string
	Address            string
	RabbitMQAddress    string
	QueueNotifications string
	QueuePurchases     string
	PostgresAddress    string
}

func MustLoad() *Config {
	return &Config{
		Env:                "local",
		Address:            os.Getenv("SERVICE_ADDRESS"),
		RabbitMQAddress:    os.Getenv("RABBITMQ_ADDRESS"),
		QueueNotifications: os.Getenv("QUEUE_NOTIFICATIONS"),
		QueuePurchases:     os.Getenv("QUEUE_PURCHASES"),
		PostgresAddress:    os.Getenv("POSTGRES_ADDRESS"),
	}
}
