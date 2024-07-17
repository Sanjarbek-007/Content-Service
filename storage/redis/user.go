package redis

import (
	pb "Content-Service/genproto/content"
	"Content-Service/storage/postgres"
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func ConnectDB() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return rdb
}

func SaveTopDestinations(ctx context.Context, Repo *postgres.ContentRepo) (*pb.Answer, error) {
	rdb := ConnectDB()

	topDestinations, err := Repo.GetTopDestinations(ctx)
	if err != nil {
		log.Println("Error fetching top destinations: ", err)
		return nil, err
	}

	id := 1
	for _, v := range topDestinations.Topdestinations {
		destination := map[string]interface{}{
			"best_time_to_visit": v.BestTimeToVisit,
			"description":        v.Description,
			"country":            v.Country,
			"popularity_score":   v.PopularityScore,
		}

		for k, v1 := range destination {
			err := rdb.HSet(ctx, strconv.Itoa(id), k, v1).Err()
			if err != nil {
				return nil, err
			}
		}
		id++
	}
	expiration := time.Until(time.Now().Add(30 * time.Minute))
	if expiration > 0 {
		err := rdb.Expire(ctx, strconv.Itoa(id), expiration).Err()
		if err != nil {
			return nil, err
		}
	}
	return getTopDestinationsFromRedis(ctx)
}

func getTopDestinationsFromRedis(ctx context.Context) (*pb.Answer, error) {
	rdb := ConnectDB()

	var topDestinations pb.Answer
	var id = 1

	for {
		key := strconv.Itoa(id)
		if rdb.Exists(ctx, key).Val() == 0 {
			break
		}

		result, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			log.Println("Error reading top destinations from Redis: ", err)
			return nil, err
		}

		popularityScore, _ := strconv.ParseInt(result["popularity_score"], 10, 64)

		topDestination := &pb.TopDestinationsRes{
			Country:         result["country"],
			Description:     result["description"],
			BestTimeToVisit: result["best_time_to_visit"],
			PopularityScore: popularityScore,
		}

		topDestinations.Topdestinations = append(topDestinations.Topdestinations, topDestination)
		id++
	}

	return &topDestinations, nil
}
