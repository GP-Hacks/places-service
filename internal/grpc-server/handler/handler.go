package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GP-Hacks/kdt2024-commons/api/proto"
	"github.com/GP-Hacks/kdt2024-places/config"
	"github.com/GP-Hacks/kdt2024-places/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log/slog"
	"math"
	"sort"
	"time"
)

type NotificationMessage struct {
	UserID  string    `json:"user_id"`
	Header  string    `json:"header"`
	Content string    `json:"content"`
	Time    time.Time `json:"time"`
}

type PurchaseMessage struct {
	UserToken    string    `json:"user_token"`
	PlaceID      int       `json:"place_id"`
	EventTime    time.Time `json:"event_time"`
	PurchaseTime time.Time `json:"purchase_time"`
	Cost         int       `json:"cost"`
}

const EarthRadius = 6371

func distance(lat1, lon1, lat2, lon2 float64) float64 {
	lat1, lon1 = toRadians(lat1), toRadians(lon1)
	lat2, lon2 = toRadians(lat2), toRadians(lon2)

	dlat := lat2 - lat1
	dlon := lon2 - lon1

	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dlon/2)*math.Sin(dlon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return EarthRadius * c
}

func toRadians(deg float64) float64 {
	return deg * math.Pi / 180
}

func roundMinutes(t time.Time) time.Time {
	minute := t.Minute()
	roundedMinute := 5 * ((minute + 4) / 5)

	if roundedMinute == 60 {
		roundedMinute = 0
		t = t.Add(time.Hour)
	}

	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), roundedMinute, 0, 0, t.Location())
}

type GRPCHandler struct {
	cfg *config.Config
	proto.UnimplementedPlacesServiceServer
	storage *storage.PostgresStorage
	logger  *slog.Logger
	mqch    *amqp.Channel
}

func NewGRPCHandler(cfg *config.Config, server *grpc.Server, storage *storage.PostgresStorage, logger *slog.Logger, mqch *amqp.Channel) *GRPCHandler {
	handler := &GRPCHandler{cfg: cfg, storage: storage, logger: logger, mqch: mqch}
	proto.RegisterPlacesServiceServer(server, handler)
	logger.Info("gRPC handler successfully registered")
	return handler
}

func (h *GRPCHandler) handleStorageError(err error, entity string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		h.logger.Warn("No "+entity+" found in the database", slog.String("entity", entity), slog.String("error", err.Error()))
		return status.Errorf(codes.NotFound, "No such "+entity+" in the database")
	}
	h.logger.Error("Error occurred while retrieving "+entity, slog.String("entity", entity), slog.String("error", err.Error()))
	return status.Errorf(codes.Internal, "An internal error occurred, please try again later")
}

func (h *GRPCHandler) GetTickets(ctx context.Context, request *proto.GetTicketsRequest) (*proto.GetTicketsResponse, error) {
	h.logger.Debug("Processing GetTickets request", slog.Any("request", request))

	select {
	case <-ctx.Done():
		h.logger.Warn("Request was cancelled by the client", slog.Any("request", request))
		return nil, ctx.Err()
	default:
	}

	userToken := request.GetToken()
	tickets, err := h.storage.GetTickets(ctx, userToken)
	if err != nil {
		return nil, h.handleStorageError(err, "tickets")
	}
	var responseTickets []*proto.Ticket
	for _, ticket := range tickets {
		responseTickets = append(responseTickets, &proto.Ticket{
			Id:        int32(ticket.ID),
			Name:      ticket.Name,
			Location:  ticket.Location,
			Timestamp: timestamppb.New(ticket.EventTime),
		})
	}

	h.logger.Info("Tickets retrieved successfully")
	return &proto.GetTicketsResponse{Response: responseTickets}, nil
}

func (h *GRPCHandler) GetPlaces(ctx context.Context, request *proto.GetPlacesRequest) (*proto.GetPlacesResponse, error) {
	h.logger.Debug("Processing GetPlaces request", slog.Any("request", request))

	select {
	case <-ctx.Done():
		h.logger.Warn("Request was cancelled by the client", slog.Any("request", request))
		return nil, ctx.Err()
	default:
	}

	var places []*storage.Place
	var err error

	category := request.GetCategory()
	if category == "all" {
		places, err = h.storage.GetPlaces(ctx)
	} else {
		places, err = h.storage.GetPlacesByCategory(ctx, category)
	}
	if err != nil {
		return nil, h.handleStorageError(err, "places")
	}

	h.logger.Info("Places retrieved from the database", slog.Int("count", len(places)))

	userLatitude := request.GetLatitude()
	userLongitude := request.GetLongitude()

	sort.Slice(places, func(i, j int) bool {
		return distance(places[i].Latitude, places[i].Longitude, userLatitude, userLongitude) <
			distance(places[j].Latitude, places[j].Longitude, userLatitude, userLongitude)
	})

	var responsePlaces []*proto.Place
	for _, place := range places {
		protoPhotos, err := h.getPlacePhotos(ctx, place.ID)
		if err != nil {
			return nil, err
		}

		times := []string{place.Time, roundMinutes(time.Now().Add(3 * time.Hour)).Format("15:04")}
		sort.Strings(times)

		responsePlaces = append(responsePlaces, &proto.Place{
			Id:          int32(place.ID),
			Category:    place.Category,
			Description: place.Description,
			Latitude:    place.Latitude,
			Longitude:   place.Longitude,
			Location:    place.Location,
			Name:        place.Name,
			Tel:         place.Tel,
			Website:     place.Website,
			Cost:        int32(place.Cost),
			Times:       times,
			Photos:      protoPhotos,
		})
	}

	h.logger.Info("Successfully processed GetPlaces request", slog.Int("response_count", len(responsePlaces)))
	return &proto.GetPlacesResponse{Response: responsePlaces}, nil
}

func (h *GRPCHandler) getPlacePhotos(ctx context.Context, placeID int) ([]*proto.Photo, error) {
	h.logger.Debug("Retrieving photos for place", slog.Int("place_id", placeID))

	placePhotos, err := h.storage.GetPhotosById(ctx, placeID)
	if err != nil {
		return nil, h.handleStorageError(err, "photos")
	}
	if placePhotos == nil {
		h.logger.Warn("No photos found for place", slog.Int("place_id", placeID))
		placePhotos = []*storage.Photo{}
	}

	var protoPhotos []*proto.Photo
	for _, placePhoto := range placePhotos {
		protoPhotos = append(protoPhotos, &proto.Photo{Url: placePhoto.Url})
	}

	h.logger.Info("Photos retrieved successfully", slog.Int("place_id", placeID), slog.Int("photo_count", len(protoPhotos)))
	return protoPhotos, nil
}

func (h *GRPCHandler) BuyTicket(ctx context.Context, request *proto.BuyTicketRequest) (*proto.BuyTicketResponse, error) {
	h.logger.Debug("Processing BuyTicket request", slog.Any("request", request))

	select {
	case <-ctx.Done():
		h.logger.Warn("Request was cancelled by the client", slog.Any("request", request))
		return nil, ctx.Err()
	default:
	}

	dbPlace, err := h.storage.GetPlaceById(ctx, int(request.GetPlaceId()))
	if err != nil {
		return nil, h.handleStorageError(err, "place")
	}

	message := NotificationMessage{
		UserID:  request.GetToken(),
		Header:  "Напоминание о покупке!",
		Content: fmt.Sprintf("Вы приобрели билет на %s в %s", dbPlace.Name, request.GetTimestamp().AsTime().Format("15:04")),
		Time:    request.GetTimestamp().AsTime().Add(-15 * time.Minute),
	}
	if err := h.publishToRabbitMQ(message, h.cfg.QueueNotifications); err != nil {
		h.logger.Error("Failed to publish notification message to RabbitMQ", slog.Any("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "An error occurred while processing your purchase")
	}
	h.logger.Info("Notification message successfully published to RabbitMQ", slog.String("queue", h.cfg.QueueNotifications))

	purchaseMessage := PurchaseMessage{
		UserToken:    request.GetToken(),
		PlaceID:      dbPlace.ID,
		EventTime:    request.GetTimestamp().AsTime(),
		PurchaseTime: time.Now(),
		Cost:         dbPlace.Cost,
	}
	if err := h.publishToRabbitMQ(purchaseMessage, h.cfg.QueuePurchases); err != nil {
		h.logger.Error("Failed to publish purchase message to RabbitMQ", slog.Any("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "An error occurred while processing your purchase")
	}
	h.logger.Info("Purchase message successfully published to RabbitMQ", slog.String("queue", h.cfg.QueuePurchases))

	err = h.storage.SaveTicket(ctx, &storage.Ticket{
		Name:      dbPlace.Name,
		Location:  dbPlace.Location,
		UserToken: request.GetToken(),
		EventTime: request.GetTimestamp().AsTime(),
	})
	if err != nil {
		h.logger.Error("Failed to save ticket", slog.Any("error", err.Error()))
		return nil, status.Errorf(codes.Internal, "An error occurred while processing your purchase")
	}

	return &proto.BuyTicketResponse{
		Response: "Ticket purchased successfully",
	}, nil
}

func (h *GRPCHandler) GetCategories(ctx context.Context, request *proto.GetCategoriesRequest) (*proto.GetCategoriesResponse, error) {
	h.logger.Debug("Processing GetCategories request")

	categories, err := h.storage.GetCategories(ctx)
	if err != nil {
		return nil, h.handleStorageError(err, "categories")
	}

	h.logger.Info("Successfully retrieved categories", slog.Int("count", len(categories)))
	return &proto.GetCategoriesResponse{Categories: categories}, nil
}

func (h *GRPCHandler) HealthCheck(ctx context.Context, req *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	h.logger.Debug("Processing HealthCheck")
	return &proto.HealthCheckResponse{
		IsHealthy: true,
	}, nil
}

func (h *GRPCHandler) publishToRabbitMQ(message interface{}, queueName string) error {
	q, err := h.mqch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		h.logger.Error("Failed to declare a queue", slog.Any("error", err.Error()))
		return err
	}
	body, err := json.Marshal(message)
	if err != nil {
		h.logger.Error("Failed to marshal message to JSON", slog.Any("error", err.Error()))
		return err
	}
	err = h.mqch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	if err != nil {
		h.logger.Error("Failed to publish a message", slog.Any("error", err.Error()))
		return err
	}
	return nil
}
