package service

import (
	pb "Content-Service/genproto"
	"Content-Service/storage/postgres"
	"context"
)

type ContentService struct {
	ContentRepo *postgres.StoryRepository
	pb.UnimplementedUserServiceServer
}

func NewContentService(repo *postgres.StoryRepository) *ContentService {
	return &ContentService{ContentRepo: repo}
}

func (service *ContentService) CreateStory(ctx context.Context, in *pb.CrateStoryRequest) (*pb.CrateStoryResponse, error) {
	return service.ContentRepo.CreateStory(ctx, in)
}

func (service *ContentService) UpdateStory(ctx context.Context, in *pb.UpdateStoryRequest) (*pb.UpdateStoryResponse, error) {
	return service.ContentRepo.UpdateStory(ctx, in)
}

func (service *ContentService) DeleteStory(ctx context.Context, in *pb.DeleteStoryRequest) (*pb.DeleteStoryResponse, error) {
	return service.ContentRepo.DeleteStory(ctx, in)
}

func (service *ContentService) GetAllStories(ctx context.Context, in *pb.GetAllStoriesRequest) (*pb.GetAllStoriesResponse, error) {
	return service.ContentRepo.GetAllStories(ctx, in)
}

func (service *ContentService) GetStoryFullInfo(ctx context.Context, in *pb.StoryFullInfoRequest) (*pb.StoryFullInfoResponse, error) {
	return service.ContentRepo.GetStoryFullInfo(ctx, in)
}

func (service *ContentService) CommentStory(ctx context.Context, in *pb.CommentStoryRequest) (*pb.CommentStoryResponse, error) {
	return service.ContentRepo.CommentStory(ctx, in)
}

func (service *ContentService) GetAllComments(ctx context.Context, in *pb.GetAllCommentRequest) (*pb.GetAllCommentResponse, error) {
	return service.ContentRepo.GetAllComments(ctx, in)
}

func (service *ContentService) CreateLike(ctx context.Context, in *pb.CreateLikeRequest) (*pb.CreateLikeResponse, error) {
	return service.ContentRepo.CreateLike(ctx, in)
}

func (service *ContentService) CreateItineraries(ctx context.Context, in *pb.CreateItinerariesRequest) (*pb.CreateItinerariesResponse, error) {
	return service.ContentRepo.CreateItineraries(ctx, in)
}

// func (service *ContentService) UpdateItineraries(ctx context.Context, in *pb.UpdateItinerariesRequest) (*pb.UpdateItinerariesResponse, error) {
// 	return service.ContentRepo.UpdateItineraries(ctx, in)
// }

func (service *ContentService) DeleteItineraries(ctx context.Context, in *pb.DeleteItinerariesRequest) (*pb.DeleteItinerariesResponse, error) {
	return service.ContentRepo.DeleteItineraries(ctx, in)
}

func (service *ContentService) GetAllItineraries(ctx context.Context, in *pb.GetAllItinerariesRequest) (*pb.GetAllItinerariesResponse, error) {
	return service.ContentRepo.GetAllItineraries(ctx, in)
}

func (service *ContentService) ItinerariesFullInfo(ctx context.Context, in *pb.ItinerariesFullInfoRequest) (*pb.ItinerariesFullInfoResponse, error) {
	return service.ContentRepo.ItinerariesFullInfo(ctx, in)
}

func (service *ContentService) CommentItineraries(ctx context.Context, in *pb.CommentItinerariesRequest) (*pb.CommentItinerariesResponse, error) {
	return service.ContentRepo.CommentItineraries(ctx, in)
}

func (service *ContentService) GetDestinations(ctx context.Context, in *pb.GetDestinationsRequest) ([]*pb.GetDestinationsResponse, error) {
	return service.ContentRepo.GetDestinations(ctx, in)
}

func (service *ContentService) GetDestinationInfo(ctx context.Context, in *pb.GetDestinationInfoRequest) (*pb.GetDestinationInfoResponse, error) {
	return service.ContentRepo.GetDestinationInfo(ctx, in)
}

func (service *ContentService) CreateSentMessage(ctx context.Context, in *pb.SentMessageRequest) (*pb.SentMessageResponse, error) {
	return service.ContentRepo.CreateSentMessage(ctx, in)
}

// func (service *ContentService) GetAllMessages(ctx context.Context, in *pb.GetAllMessagesRequest) (*pb.GetAllMessagesResponse, error) {
// 	return service.ContentRepo.GetAllMessages(ctx, in)
// }

