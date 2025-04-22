package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"math/rand"
	"net/http"
	"time"
)

type PostgresStorage struct {
	db *pgxpool.Pool
}

type Place struct {
	ID          int
	Category    string
	Description string
	Latitude    float64
	Longitude   float64
	Location    string
	Name        string
	Tel         string
	Website     string
	Cost        int
	Time        string
}

type Ticket struct {
	ID        int
	Name      string
	Location  string
	UserToken string
	EventTime time.Time
}

type Photo struct {
	PlaceID int
	Url     string
}

func NewPostgresStorage(storagePath string) (*PostgresStorage, error) {
	const op = "storage.postgresql.New"
	dbpool, err := pgxpool.New(context.Background(), storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to create database connection pool: %w", op, err)
	}
	return &PostgresStorage{db: dbpool}, nil
}

func (s *PostgresStorage) Close() {
	s.db.Close()
}

func (s *PostgresStorage) GetPlaces(ctx context.Context) ([]*Place, error) {
	const op = "storage.postgresql.GetPlaces"
	query := "SELECT id, category, description, latitude, longitude, location, name, tel, website, cost, time FROM places"
	places, err := s.fetchPlaces(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to fetch places: %w", op, err)
	}
	return places, nil
}

func (s *PostgresStorage) GetPlacesByCategory(ctx context.Context, category string) ([]*Place, error) {
	const op = "storage.postgresql.GetPlacesByCategory"
	query := "SELECT id, category, description, latitude, longitude, location, name, tel, website, cost, time FROM places WHERE category = $1"
	places, err := s.fetchPlaces(ctx, query, category)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to fetch places by category '%s': %w", op, category, err)
	}
	return places, nil
}

func (s *PostgresStorage) GetPlaceById(ctx context.Context, placeID int) (*Place, error) {
	const op = "storage.postgresql.GetPlaceById"
	query := "SELECT id, category, description, latitude, longitude, location, name, tel, website, cost, time FROM places WHERE id = $1"
	place := &Place{}
	err := s.db.QueryRow(ctx, query, placeID).Scan(
		&place.ID, &place.Category, &place.Description, &place.Latitude, &place.Longitude,
		&place.Location, &place.Name, &place.Tel, &place.Website, &place.Cost, &place.Time,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%s: place with ID %d not found: %w", op, placeID, err)
		}
		return nil, fmt.Errorf("%s: failed to fetch place with ID %d: %w", op, placeID, err)
	}
	return place, nil
}

func (s *PostgresStorage) GetPhotosById(ctx context.Context, placeID int) ([]*Photo, error) {
	const op = "storage.postgresql.GetPhotosById"
	query := "SELECT place_id, url FROM photos WHERE place_id = $1"
	rows, err := s.db.Query(ctx, query, placeID)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to query photos for place ID %d: %w", op, placeID, err)
	}
	defer rows.Close()

	var photos []*Photo
	for rows.Next() {
		photo := &Photo{}
		err := rows.Scan(&photo.PlaceID, &photo.Url)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to scan photo for place ID %d: %w", op, placeID, err)
		}
		photos = append(photos, photo)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: error occurred while iterating over photos for place ID %d: %w", op, placeID, err)
	}
	if len(photos) == 0 {
		fmt.Printf("No photos found for place ID %d\n", placeID)
	}
	return photos, nil
}

func (s *PostgresStorage) GetCategories(ctx context.Context) ([]string, error) {
	const op = "storage.postgresql.GetCategories"
	rows, err := s.db.Query(ctx, "SELECT DISTINCT category FROM places")
	if err != nil {
		return nil, fmt.Errorf("%s: failed to query categories: %w", op, err)
	}
	defer rows.Close()

	var categories []string
	for rows.Next() {
		var category string
		if err := rows.Scan(&category); err != nil {
			return nil, fmt.Errorf("%s: failed to scan category: %w", op, err)
		}
		categories = append(categories, category)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: error occurred while iterating over categories: %w", op, err)
	}

	if len(categories) == 0 {
		fmt.Println("No categories found in the database")
	}
	return categories, nil
}

func (s *PostgresStorage) GetTickets(ctx context.Context, userToken string) ([]*Ticket, error) {
	const op = "storage.postgresql.GetTickets"
	rows, err := s.db.Query(ctx, `SELECT id, name, location, user_token, event_time FROM tickets WHERE user_token = $1`, userToken)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to query tickets: %w", op, err)
	}
	var tickets []*Ticket
	for rows.Next() {
		var ticket Ticket
		if err := rows.Scan(&ticket.ID, &ticket.Name, &ticket.Location, &ticket.UserToken, &ticket.EventTime); err != nil {
			return nil, fmt.Errorf("%s: failed to scan category: %w", op, err)
		}
		tickets = append(tickets, &ticket)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: error occurred while iterating over tickets: %w", op, err)
	}

	if len(tickets) == 0 {
		return nil, pgx.ErrNoRows
	}
	return tickets, nil
}

func (s *PostgresStorage) SaveTicket(ctx context.Context, ticket *Ticket) error {
	const op = "storage.postgresql.SaveTicket"
	_, err := s.db.Exec(ctx, `INSERT INTO tickets (name, location, user_token, event_time) VALUES ($1, $2, $3, $4)`, ticket.Name, ticket.Location, ticket.UserToken, ticket.EventTime)
	if err != nil {
		return fmt.Errorf("%s: failed to save ticket: %w", op, err)
	}
	return nil
}

func (s *PostgresStorage) fetchPlaces(ctx context.Context, query string, args ...interface{}) ([]*Place, error) {
	const op = "storage.postgresql.fetchPlaces"
	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to execute query: %w", op, err)
	}
	defer rows.Close()

	var places []*Place
	for rows.Next() {
		place := &Place{}
		err := rows.Scan(
			&place.ID, &place.Category, &place.Description, &place.Latitude, &place.Longitude,
			&place.Location, &place.Name, &place.Tel, &place.Website, &place.Cost, &place.Time,
		)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to scan place: %w", op, err)
		}
		places = append(places, place)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: error occurred while iterating over places: %w", op, err)
	}
	if len(places) == 0 && len(args) > 0 {
		fmt.Printf("No places found for query with arguments %v\n", args)
		return nil, pgx.ErrNoRows
	}
	return places, nil
}

func (s *PostgresStorage) CreateTables(ctx context.Context) error {
	const op = "storage.postgresql.CreateTables"
	tables := []string{
		`CREATE TABLE IF NOT EXISTS places (
			id SERIAL PRIMARY KEY,
			category VARCHAR(255),
			description TEXT,
			latitude DOUBLE PRECISION,
			longitude DOUBLE PRECISION,
			location TEXT,
			name VARCHAR(255),
			tel VARCHAR(50),
			website VARCHAR(255),
			cost INT,
			time VARCHAR(50)
		)`,
		`CREATE TABLE IF NOT EXISTS photos (
			place_id INT REFERENCES places(id) ON DELETE CASCADE,
			url TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS tickets (
			id SERIAL PRIMARY KEY,
			name TEXT,
			location TEXT,
			user_token VARCHAR(255),
			event_time TIMESTAMP
		)`,
	}

	for _, table := range tables {
		if _, err := s.db.Exec(ctx, table); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}
	return nil
}

func (s *PostgresStorage) FetchAndStoreData(ctx context.Context) error {
	const op = "storage.postgresql.FetchAndStoreData"
	url := "https://api.foursquare.com/v3/places/search?categories=10001%2C10002%2C10004%2C10009%2C10027%2C10028%2C10029%2C10030%2C10031%2C10044%2C10046%2C10047%2C10056%2C10058%2C10059%2C10068%2C10069%2C16005%2C16011%2C16020%2C16025%2C16026%2C16031%2C16034%2C16035%2C16038%2C16039%2C16041%2C16046%2C16047%2C16052&exclude_all_chains=true&fields=categories%2Cname%2Cdescription%2Cgeocodes%2Clocation%2Ctel%2Cphotos%2Cwebsite&polygon=54.9887%2C48.0821~56.2968%2C49.1917~56.5096%2C50.3453~55.8923%2C51.4659~55.7380%2C54.0586~55.1836%2C53.0369~54.3534%2C53.2347~54.7675%2C51.1912~54.9193%2C49.2466~54.6405%2C48.6644&limit=50"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("%s: failed to create HTTP request: %w", op, err)
	}
	req.Header.Add("accept", "application/json")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Add("Authorization", "fsq3VM2gW4VslOMC96mTH1K/2xXH65KOnIO/TU8GiPI4Oic=")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: failed to execute HTTP request: %w", op, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: unexpected status code %d from API: %w", op, res.StatusCode, err)
	}

	var count int
	err = s.db.QueryRow(ctx, `SELECT COUNT(*) FROM places`).Scan(&count)
	if err != nil {
		return fmt.Errorf("%s: failed to count existing places: %w", op, err)
	}

	if count > 0 {
		fmt.Println("Data already exists in the database. Skipping fetch and store.")
		return nil
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("%s: failed to read response body: %w", op, err)
	}

	var apiResponse struct {
		Results []struct {
			Categories  []struct{ Name string } `json:"categories"`
			Description string                  `json:"description,omitempty"`
			Geocodes    struct {
				Main struct {
					Latitude  float64 `json:"latitude"`
					Longitude float64 `json:"longitude"`
				} `json:"main"`
			} `json:"geocodes"`
			Location struct {
				FormattedAddress string `json:"formatted_address"`
			} `json:"location"`
			Name    string `json:"name"`
			Tel     string `json:"tel,omitempty"`
			Website string `json:"website,omitempty"`
			Photos  []struct {
				Prefix string `json:"prefix"`
				Suffix string `json:"suffix"`
			} `json:"photos,omitempty"`
		} `json:"results"`
	}

	if err := json.Unmarshal(body, &apiResponse); err != nil {
		return fmt.Errorf("%s: failed to parse API response: %w", op, err)
	}

	var places []Place
	var photos []Photo

	for i, place := range apiResponse.Results {
		category := place.Categories[0].Name
		if category == "Историческое место или особо охраняемая территория" {
			category = "Историческое место"
		}
		dbPlace := Place{
			ID:          i + 1,
			Category:    category,
			Description: place.Description,
			Latitude:    place.Geocodes.Main.Latitude,
			Longitude:   place.Geocodes.Main.Longitude,
			Location:    place.Location.FormattedAddress,
			Name:        place.Name,
			Tel:         place.Tel,
			Website:     place.Website,
			Cost:        200 + rand.Intn(500),
			Time:        fmt.Sprintf("%02d:00", rand.Intn(11)+10),
		}
		places = append(places, dbPlace)
		for _, photo := range place.Photos {
			dbPhoto := Photo{
				PlaceID: dbPlace.ID,
				Url:     photo.Prefix + "original" + photo.Suffix,
			}
			photos = append(photos, dbPhoto)
		}
	}

	fmt.Printf("Fetched %d places and %d photos from API\n", len(places), len(photos))

	for _, place := range places {
		_, err := s.db.Exec(ctx, `
			INSERT INTO places (category, description, latitude, longitude, location, name, tel, website, cost, time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			place.Category, place.Description, place.Latitude, place.Longitude, place.Location, place.Name, place.Tel, place.Website, place.Cost, place.Time)
		if err != nil {
			return fmt.Errorf("%s: failed to insert place into database: %w", op, err)
		}
	}

	for _, photo := range photos {
		_, err := s.db.Exec(ctx, `INSERT INTO photos (place_id, url) VALUES ($1, $2)`, photo.PlaceID, photo.Url)
		if err != nil {
			return fmt.Errorf("%s: failed to insert photo into database: %w", op, err)
		}
	}

	fmt.Println("Data successfully fetched and stored")
	return nil
}
