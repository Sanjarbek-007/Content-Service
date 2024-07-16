package redis

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
	pb "Content-Service/genproto"

	"github.com/redis/go-redis/v9"
)

type UserRepository struct {
	RD *redis.Client
	DB *sql.DB
}

func NewUserRepository(rd *redis.Client, db *sql.DB) *UserRepository {
	return &UserRepository{
		RD: rd,
		DB: db,
	}
}

func (repo *UserRepository) GetTopDestinations(ctx context.Context, request *pb.GetTrendingDestinationsRequest) (*pb.GetTrendingDestinationsResponse, error) {

	destinationsJSON, err := repo.RD.Get(ctx, "top_destinations").Bytes()
	if err == nil {
		var destinations pb.GetTrendingDestinationsResponse
		if err := json.Unmarshal(destinationsJSON, &destinations); err != nil {
			return nil, err
		}
		return destinations, nil
	} else if err != redis.Nil {
		return nil, err
	}

	destinations, err := repo.fetchDestinationsFromDB(ctx)
	if err != nil {
		return nil, err
	}

	if err := repo.cacheDestinations(ctx, destinations); err != nil {
		return nil, err
	}

	return destinations, nil
}

func (repo *UserRepository) fetchDestinationsFromDB(ctx context.Context) ([]Destination, error) {
	rows, err := repo.DB.QueryContext(ctx, `
		SELECT id, name, country, description, best_time_to_visit, average_cost_per_day, currency, language, created_at, updated_at
		FROM destinations
		ORDER BY popularity_score DESC
		LIMIT 10
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var destinations []Destination
	for rows.Next() {
		var dest Destination
		err := rows.Scan(
			&dest.ID, &dest.Name, &dest.Country, &dest.Description, &dest.BestTimeToVisit,
			&dest.AverageCostPerDay, &dest.Currency, &dest.Language, &dest.CreatedAt, &dest.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		destinations = append(destinations, dest)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return destinations, nil
}

func (repo *UserRepository) cacheDestinations(ctx context.Context, destinations []Destination) error {
	destinationsJSON, err := json.Marshal(destinations)
	if err != nil {
		return err
	}

	err = repo.RD.Set(ctx, "top_destinations", destinationsJSON, 1*time.Hour).Err()
	if err != nil {
		return err
	}

	return nil
}
