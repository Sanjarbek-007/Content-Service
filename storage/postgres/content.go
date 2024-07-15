package postgres

import (
	storage "Content-Service/help"
	"context"
	"database/sql"
	"fmt"
	"time"

	pb "Content-Service/genproto"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type StoryRepository struct {
	Db *sql.DB
}

func NewStoryRepository(db *sql.DB) *StoryRepository {
	return &StoryRepository{Db: db}
}

var logger *zap.Logger

func (repo *StoryRepository) CreateStory(ctx context.Context, request *pb.CrateStoryRequest) (*pb.CrateStoryResponse, error) {
	tx, err := repo.Db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error("failed to begin transaction")
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}

	query := `
		INSERT INTO stories (id, title, content, location, author_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, current_timestamp, current_timestamp)
		RETURNING created_at
	`
	id := uuid.NewString()
	var createdAt time.Time
	err = tx.QueryRow(query, id, request.Title, request.Content, request.Location, request.AuthorId).
		Scan(&createdAt)
	if err != nil {
		tx.Rollback()
		logger.Error("error creating story")
		return nil, fmt.Errorf("error creating story: %v", err)
	}

	tagQuery := "INSERT INTO story_tags (story_id, tag) VALUES ($1, $2)"
	for _, tag := range request.Tags {
		_, err = tx.ExecContext(ctx, tagQuery, id, tag)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("error inserting story tag: %v", err)
		}
	}

	if err = tx.Commit(); err != nil {
		logger.Error("failed to commit transaction")
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	return &pb.CrateStoryResponse{
		Id:        id,
		Title:     request.Title,
		Content:   request.Content,
		Location:  request.Location,
		Tags:      request.Tags,
		AuthorId:  request.AuthorId,
		CreatedAt: createdAt.Format("2006-01-02 15:04:05"),
	}, nil
}

func (repo *StoryRepository) UpdateStory(ctx context.Context, request *pb.UpdateStoryRequest) (*pb.UpdateStoryResponse, error) {
	query := `
		UPDATE stories
		SET title = $2, content = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		RETURNING id, title, content, location, author_id, created_at, updated_at
	`
	var id, title, content, location, authorID string
	var createdAt, updatedAt string
	err := repo.Db.QueryRow(query, request.StoryId, request.Title, request.Content).
		Scan(&id, &title, &content, &location, &authorID, &createdAt, &updatedAt)
	if err != nil {
		logger.Error("error updating story")
		return nil, fmt.Errorf("error updating story: %v", err)
	}

	return &pb.UpdateStoryResponse{
		Id:        id,
		Title:     title,
		Content:   content,
		Location:  location,
		AuthorId:  authorID,
		UpdatedAt: updatedAt,
	}, nil
}

func (repo *StoryRepository) DeleteStory(ctx context.Context, request *pb.DeleteStoryRequest) (*pb.DeleteStoryResponse, error) {
	query := `
		DELETE FROM stories
		WHERE id = $1
	`
	result, err := repo.Db.Exec(query, request.StoryId)
	if err != nil {
		logger.Error("error deleting story")
		return nil, fmt.Errorf("error deleting story: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()

	return &pb.DeleteStoryResponse{
		MessageStory: rowsAffected > 0,
	}, nil
}

func (repo *StoryRepository) GetAllStories(ctx context.Context, request *pb.GetAllStoriesRequest) (*pb.GetAllStoriesResponse, error) {
	var (
		params = make(map[string]interface{})
		arr    []interface{}
		limit  string
		offset string
	)
	filter := ""
	if len(request.AuthorId) > 0 {
		params["user_name"] = request.AuthorId
		filter += " and user_name = :user_name "
	}
	if request.Limit > 0 {
		params["limit"] = request.Limit
		limit = " LIMIT :limit"
	}
	if request.Offset > 0 {
		params["offset"] = request.Offset
		offset += `OFFSET  :offset`
	}

	query := "select user_name,password ,email from users  where  deleted_at is null "

	query = query + filter + limit + offset
	query, arr = storage.ReplaceQueryParams(query, params)
	rows, err := repo.Db.Query(query, arr...)
	if err != nil {
		return nil, err
	}
	var users []*pb.Story
	for rows.Next() {
		var storyResponse pb.Story
		err := rows.Scan(&storyResponse.Id, &storyResponse.Title, &storyResponse.Author, &storyResponse)
		if err != nil {
			return nil, err
		}
		users = append(users, &storyResponse)
	}
	return &pb.GetAllStoriesResponse{Stories: users}, nil
}

func (repo *StoryRepository) GetStoryFullInfo(ctx context.Context, request *pb.StoryFullInfoRequest) (*pb.StoryFullInfoResponse, error) {
	query := `
		SELECT id, title, content, location, author_id, likes_count, comments_count, created_at, updated_at
		FROM stories
		WHERE deleted_at IS NULL AND id = $1
	`
	var id, title, content, location, authorID string
	var likesCount, commentsCount int32
	var createdAt, updatedAt string
	err := repo.Db.QueryRow(query, request.StoryId).
		Scan(&id, &title, &content, &location, &authorID, &likesCount, &commentsCount, &createdAt, &updatedAt)
	if err != nil {
		logger.Error("error fetching story")
		return nil, fmt.Errorf("error fetching story: %v", err)
	}

	authorQuery := `
		SELECT username, full_name
		FROM users
		WHERE id = $1
	`
	var username, fullName string
	err = repo.Db.QueryRow(authorQuery, authorID).Scan(&username, &fullName)
	if err != nil {
		logger.Error("error fetching author")
		return nil, fmt.Errorf("error fetching author: %v", err)
	}

	tagsQuery := `
		SELECT tag
		FROM story_tags
		WHERE story_id = $1
	`
	rows, err := repo.Db.Query(tagsQuery, id)
	if err != nil {
		logger.Error("error fetching tags")
		return nil, fmt.Errorf("error fetching tags: %v", err)
	}
	defer rows.Close()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			logger.Error("error scanning tag", zap.Error(err))
			return nil, fmt.Errorf("error scanning tag: %v", err)
		}
		tags = append(tags, tag)
	}

	response := &pb.StoryFullInfoResponse{
		Id:            id,
		Title:         title,
		Content:       content,
		Location:      location,
		Tags:          tags,
		Author:        &pb.Author{Id: authorID, Username: username, FullName: fullName},
		LikesCount:    likesCount,
		CommentsCount: commentsCount,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}

	return response, nil
}

func (repo *StoryRepository) CommentStory(ctx context.Context, request *pb.CommentStoryRequest) (*pb.CommentStoryResponse, error) {
	query := `
		INSERT INTO comments (id, content, author_id, story_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		WHERE deleted_at IS NULL
		RETURNING created_at
	`
	id := uuid.NewString()
	var createdAt string
	err := repo.Db.QueryRow(query, id, request.Content, request.AuthorId, request.StoryId).
		Scan(&id, &createdAt)
	if err != nil {
		logger.Error("error commenting on story")
		return nil, fmt.Errorf("error commenting on story: %v", err)
	}

	return &pb.CommentStoryResponse{
		Id:        id,
		Content:   request.Content,
		AuthorId:  request.AuthorId,
		StoryId:   request.StoryId,
		CreatedAt: createdAt,
	}, nil
}

func (repo *StoryRepository) GetAllComments(ctx context.Context, request *pb.GetAllCommentRequest) (*pb.GetAllCommentResponse, error) {
	query := `
		SELECT id, content, author_id, created_at
		FROM comments
		WHERE story_id = $1
		LIMIT $2 OFFSET $3
		and deleted_at IS NULL
	`
	var (
		params = make(map[string]interface{})
		arr    []interface{}
		limit  string
		offset string
	)
	filter := ""
	if len(request.StoryId) > 0 {
		params["story_id"] = request.StoryId
		filter += " and story_id = :story_id "
	}
	if request.Limit > 0 {
		params["limit"] = request.Limit
		limit = " LIMIT :limit"
	}
	if request.Offset > 0 {
		params["offset"] = request.Offset
		offset += `OFFSET  :offset`
	}

	query = query + filter + limit + offset
	query, arr = storage.ReplaceQueryParams(query, params)
	rows, err := repo.Db.Query(query, arr...)
	if err != nil {
		logger.Error("Error in GetAllComments")
		return nil, err
	}
	var comments []*pb.Comment
	for rows.Next() {
		var comment pb.Comment
		err := rows.Scan(&comment.Id, &comment.Content, &comment.Author, &comment.CreatedAt)
		if err != nil {
			logger.Error("Error in GetAllComments scan")
			return nil, err
		}
		comments = append(comments, &comment)
	}
	return &pb.GetAllCommentResponse{Comment: comments}, nil
}

func (repo *StoryRepository) CreateLike(ctx context.Context, request *pb.CreateLikeRequest) (*pb.CreateLikeResponse, error) {
	query := `
		INSERT INTO likes (story_id, user_id, liked_at, updated_at)
		VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING story_id, user_id, liked_at
	`
	var storyID, userID, likedAt string
	err := repo.Db.QueryRow(query, request.StoryId, request.UserId).
		Scan(&storyID, &userID, &likedAt)
	if err != nil {
		logger.Error("error in creating Like")
		return nil, fmt.Errorf("error creating like: %v", err)
	}

	return &pb.CreateLikeResponse{
		StoryId: storyID,
		UserId:  userID,
		LikedAt: likedAt,
	}, nil
}

func (repo *StoryRepository) CreateItineraries(ctx context.Context, request *pb.CreateItinerariesRequest) (*pb.CreateItinerariesResponse, error) {
	query := `
		INSERT INTO itineraries (title, description, start_date, end_date, destinations)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING title, description, start_date, end_date, author_id, created_at`

	id := uuid.NewString()
	var Title, Description, startDate, endDate, AuthorId, CreatedAt string

	err := repo.Db.QueryRow(query, request.Title, request.Description, request.StartDate, request.EndDate, request.Destinations).
		Scan(&Title, &Description, &startDate, &endDate, &AuthorId, &CreatedAt)
	if err != nil {
		logger.Error("error in creating Like")
		return nil, fmt.Errorf("error creating like: %v", err)
	}

	return &pb.CreateItinerariesResponse{
		Id:          id,
		Title:       Title,
		Description: Description,
		StartDate:   startDate,
		EndDate:     endDate,
		AuthorId:    AuthorId,
		CreatedAt:   CreatedAt,
	}, nil
}

func (repo *StoryRepository) UpdateItineraries(ctx context.Context, request *pb.UpdateItinerariesRequest) (*pb.UpdateItinerariesResponse, error) {
	query := `UPDATE itineraries SET title = $1, description = $2, updated_at = $3 where deleted_at is null and id = $4
			RETURNING start_date, end_date, author_id, updated_at`

	var startDate, endDate, authorId, updatedAt string
	err := repo.Db.QueryRow(query, request.Title, request.Description, time.Now(), request.ItineraryId).Scan(&startDate, &endDate, &authorId, &updatedAt)
	if err != nil {
		logger.Error("error UpdateItineraries")
		return nil, fmt.Errorf("error UpdateItineraries: %v", err)
	}

	return &pb.UpdateItinerariesResponse{
		Id:          request.ItineraryId,
		Title:       request.Title,
		Description: request.Description,
		StartDate:   startDate,
		EndDate:     endDate,
		AuthorId:    authorId,
		UpdatedAt:   updatedAt,
	}, nil
}

func (repo *StoryRepository) DeleteItineraries(ctx context.Context, request *pb.DeleteItinerariesRequest) (*pb.DeleteItinerariesResponse, error) {
	query := `UPDATE itineraries SET deleted_at = $1 where deleted_at is null and id = $2`
	result, err := repo.Db.Exec(query, time.Now(), request.ItineraryId)
	if err != nil {
		logger.Error("error UpdateItineraries")
		return nil, fmt.Errorf("error UpdateItineraries: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()

	return &pb.DeleteItinerariesResponse{
		MessageItinerary: rowsAffected > 0,
	}, nil
}

func (repo *StoryRepository) GetAllItineraries(ctx context.Context, request *pb.GetAllItinerariesRequest) (*pb.GetAllItinerariesResponse, error) {
	query := `SELECT id, title, author_id, start_date, end_date, likes_count, comments_count, created_at from itineraries where deleted_at is null`

	var (
		params = make(map[string]interface{})
		arr    []interface{}
		limit  string
		offset string
	)
	filter := ""
	if len(request.ItineraryId) > 0 {
		params["id"] = request.ItineraryId
		filter += " and id = :id "
	}
	if request.Limit > 0 {
		params["limit"] = request.Limit
		limit = " LIMIT :limit"
	}
	if request.Offset > 0 {
		params["offset"] = request.Offset
		offset += `OFFSET  :offset`
	}

	query = query + filter + limit + offset
	query, arr = storage.ReplaceQueryParams(query, params)
	rows, err := repo.Db.Query(query, arr...)
	if err != nil {
		logger.Error("Error in GetAllItineraries")
		return nil, err
	}
	var Itineraries []*pb.Itinerary
	for rows.Next() {
		var Itinerary pb.Itinerary
		err := rows.Scan(&Itinerary.Id, &Itinerary.Title, &Itinerary.Author, &Itinerary.StartDate, &Itinerary.EndDate, &Itinerary.LikesCount, &Itinerary.CommentsCount, &Itinerary.CreatedAt)
		if err != nil {
			logger.Error("Error in GetAllItineraries scan")
			return nil, err
		}
		Itineraries = append(Itineraries, &Itinerary)
	}
	return &pb.GetAllItinerariesResponse{Itineraries: Itineraries}, nil
}

func (repo *StoryRepository) ItinerariesFullInfo(ctx context.Context, request *pb.ItinerariesFullInfoRequest) (*pb.ItinerariesFullInfoResponse, error) {
	itineraryQuery := `
		SELECT id, title, description, start_date, end_date, author_id, likes_count, comments_count, created_at, updated_at
		FROM itineraries
		WHERE id = $1 AND deleted_at IS NULL
	`
	var itinerary pb.ItinerariesFullInfoResponse
	var authorId string

	err := repo.Db.QueryRow(itineraryQuery, request.ItineraryId).Scan(
		&itinerary.Id,
		&itinerary.Title,
		&itinerary.Description,
		&itinerary.StartDate,
		&itinerary.EndDate,
		&authorId,
		&itinerary.LikesCount,
		&itinerary.CommentsCount,
		&itinerary.CreatedAt,
		&itinerary.UpdatedAt,
	)
	if err != nil {
		logger.Error("error fetching itinerary details in ItinerariesFullInfo")
		return nil, fmt.Errorf("error fetching itinerary details: %v", err)
	}

	authorQuery := `
		SELECT id, username, full_name
		FROM users
		WHERE id = $1
	`
	var author pb.Author
	err = repo.Db.QueryRow(authorQuery, authorId).Scan(
		&author.Id,
		&author.Username,
		&author.FullName,
	)
	if err != nil {
		logger.Error("error fetching itinerary details in ItinerariesFullInfo")
		return nil, fmt.Errorf("error fetching author details: %v", err)
	}
	itinerary.Author = &author

	destinationsQuery := `
		SELECT id, name, start_date, end_date
		FROM itinerary_destinations
		WHERE itinerary_id = $1
	`
	rows, err := repo.Db.Query(destinationsQuery, request.ItineraryId)
	if err != nil {
		logger.Error("error fetching itinerary details in ItinerariesFullInfo")
		return nil, fmt.Errorf("error fetching destinations: %v", err)
	}
	defer rows.Close()

	var destinations []*pb.Destination
	for rows.Next() {
		var destination pb.Destination
		var destinationId string

		err := rows.Scan(
			&destinationId,
			&destination.Name,
			&destination.StartDate,
			&destination.EndDate,
		)
		if err != nil {
			logger.Error("error scanning destination in ItinerariesFullInfo")
			return nil, fmt.Errorf("error scanning destination: %v", err)
		}

		activitiesQuery := `
			SELECT activity
			FROM itinerary_activities
			WHERE destination_id = $1
		`
		activityRows, err := repo.Db.QueryContext(ctx, activitiesQuery, destinationId)
		if err != nil {
			logger.Error("error fetching itinerary details in ItinerariesFullInfo")
			return nil, fmt.Errorf("error fetching activities: %v", err)
		}
		defer activityRows.Close()

		var activities []string
		for activityRows.Next() {
			var activity string
			err := activityRows.Scan(&activity)
			if err != nil {
				logger.Error("error fetching itinerary details in ItinerariesFullInfo")
				return nil, fmt.Errorf("error scanning activity: %v", err)
			}
			activities = append(activities, activity)
		}
		destination.Activities = activities

		destinations = append(destinations, &destination)
	}
	itinerary.Destinations = destinations

	return &itinerary, nil
}

func (repo *StoryRepository) CommentItineraries(ctx context.Context, request *pb.CommentItinerariesRequest) (*pb.CommentItinerariesResponse, error) {
	query := `INSERT INTO comments (id, story_id, content, author_id)
		VALUES($1, $2, $3, $4)
		RETURNING story_id, content, author_id, created_at`

	id := uuid.NewString()
	var storyId, content, authorId, createdAt string

	err := repo.Db.QueryRow(query, id, request.ItineraryId, request.Content, request.AuthorId).
		Scan(&storyId, &content, &authorId, &createdAt)
	if err != nil {
		logger.Error("error in CommentItineraries")
		return nil, fmt.Errorf("error CommentItineraries: %v", err)
	}

	return &pb.CommentItinerariesResponse{
		Id:          id,
		Content:     content,
		AuthorId:    authorId,
		ItineraryId: storyId,
		CreatedAt:   createdAt,
	}, nil
}

func (repo *StoryRepository) GetDestinations(ctx context.Context, request *pb.GetDestinationsRequest) ([]*pb.GetDestinationsResponse, error) {
	query := `SELECT id, name, country, description from destinations where deleted_at is null `

	var (
		params = make(map[string]interface{})
		arr    []interface{}
		limit  string
		offset string
	)
	filter := ""
	if len(request.Country) > 0 {
		params["country"] = request.Country
		filter += " and country = :country "
	}
	if len(request.City) > 0 {
		params["name"] = request.City
		filter += " and name = :name "
	}
	if request.Limit > 0 {
		params["limit"] = request.Limit
		limit = " LIMIT :limit"
	}
	if request.Offset > 0 {
		params["offset"] = request.Offset
		offset += `OFFSET  :offset`
	}

	query = query + filter + limit + offset
	query, arr = storage.ReplaceQueryParams(query, params)
	rows, err := repo.Db.Query(query, arr...)
	if err != nil {
		logger.Error("Error in GetDestinations")
		return nil, err
	}
	var destinations []*pb.GetDestinationsResponse
	for rows.Next() {
		var destination pb.GetDestinationsResponse
		err := rows.Scan(&destination.Id, &destination.Name, &destination.Country, &destination.Description, &destination.PopularActivities)
		if err != nil {
			logger.Error("Error in GetDestinations scan")
			return nil, err
		}
		destinations = append(destinations, &destination)
	}

	return destinations, nil
}

func (repo *StoryRepository) GetDestinationInfo(ctx context.Context, request *pb.GetDestinationInfoRequest) (*pb.GetDestinationInfoResponse, error) {
	query := `SELECT id, name, country, description, best_time_to_visit, average_cost_per_day, currency, language
		from destinations where deleted_at is null and id = $1`

	var id, name, country, description, bestTimeToVisit, currency, language string
	var averageCostPerDay int32

	err := repo.Db.QueryRow(query, request.DestinationId).Scan(&id, &name, &country, &description, &bestTimeToVisit, &averageCostPerDay, &currency, &language)
	if err != nil {
		logger.Error("error scanning destination in GetDestinationInfo")
		return nil, fmt.Errorf("error scanning destination: %v", err)
	}

	return &pb.GetDestinationInfoResponse{
		Id:                id,
		Name:              name,
		Country:           country,
		Description:       description,
		BestTimeToVisit:   bestTimeToVisit,
		AverageCostPerDay: averageCostPerDay,
		Currency:          currency,
		Language:          language,
	}, nil
}

func (repo *StoryRepository) CreateSentMessage(ctx context.Context, request *pb.SentMessageRequest) (*pb.SentMessageResponse, error) {
	query := `INSERT INTO messages (id, sender_id, recipient_id, content, created_at)
		VALUES ($1, $2, $3, $4, current_timestamp)
		RETURNING sender_id, recipient_id, content, created_at`

	id := uuid.NewString()
	var senderId, recipientId, content, createdAt string

	err := repo.Db.QueryRow(query, id, request.SenderId, request.RecipientId, request.Content).
		Scan(&senderId, &recipientId, &content, &createdAt)
	if err != nil {
		logger.Error("error in CreateSentMessage")
		return nil, fmt.Errorf("error CreateSentMessage: %v", err)
	}

	return &pb.SentMessageResponse{
		Id:          id,
		SenderId:    senderId,
		RecipientId: recipientId,
		Content:     content,
		CreatedAt:   createdAt,
	}, nil
}

// func (repo *StoryRepository) GetAllMessages(ctx context.Context, request *pb.GetAllMessagesRequest) (*pb.GetAllMessagesResponse, error) {

// }

func (repo *StoryRepository) CreateTravelTip(ctx context.Context, request *pb.CreateTravelTipRequest) (*pb.CreateTravelTipResponse, error) {
	query := `INSERT INTO travel_tips (id, title, content, category, author_id, created_at)
			VALUES($1, $2, $3, $4, $5, current_timestamp)
			RETURNING title, content, category, author_id, created_at`

	id := uuid.NewString()
	var title, content, category, authorId, createdAt string

	err := repo.Db.QueryRow(query, id, request.Title, request.Content, request.Category, request.AuthorId).
		Scan(&title, &content, &category, &authorId, createdAt)
	if err != nil {
		logger.Error("error in CreateTravelTip")
		return nil, fmt.Errorf("error CreateTravelTip: %v", err)
	}

	return &pb.CreateTravelTipResponse{
		Id:        id,
		Title:     title,
		Content:   content,
		Category:  category,
		AuthorId:  authorId,
		CreatedAt: createdAt,
	}, nil
}

func (repo *StoryRepository) GetTravelTips(ctx context.Context, request *pb.GetTravelTipsRequest) (*pb.GetTravelTipsResponse, error) {
	query := `SELECT id, title, category, author_id, created_at from travel_tips where deleted_at is null`

	var (
		params = make(map[string]interface{})
		arr    []interface{}
		limit  string
		offset string
	)
	filter := ""
	if request.Limit > 0 {
		params["limit"] = request.Limit
		limit = " LIMIT :limit"
	}
	if request.Offset > 0 {
		params["offset"] = request.Offset
		offset += `OFFSET  :offset`
	}

	query = query + filter + limit + offset
	query, arr = storage.ReplaceQueryParams(query, params)
	rows, err := repo.Db.Query(query, arr...)
	if err != nil {
		logger.Error("Error in GetTravelTips")
		return nil, err
	}
	var tips []*pb.Tip
	for rows.Next() {
		var tip pb.Tip
		err := rows.Scan(&tip.Id, &tip.Title, &tip.Category, &tip.Author, &tip.CreatedAt)
		if err != nil {
			logger.Error("Error in GetDestinations scan")
			return nil, err
		}
		tips = append(tips, &tip)
	}

	return &pb.GetTravelTipsResponse{Tips: tips}, nil
}
