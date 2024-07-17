package service

import (
	pb "Content-Service/genproto/content"
	"Content-Service/logger"
	"Content-Service/storage/postgres"
	"Content-Service/storage/redis"
	"context"
	"database/sql"
	"log/slog"
)

type ContentService struct {
	pb.UnimplementedContentServer
	Repo *postgres.ContentRepo
	Log  *slog.Logger
}

func NewContentService(db *sql.DB) *ContentService {
	return &ContentService{
		Repo: postgres.NewContentRepository(db),
		Log:  logger.NewLogger(),
	}
}
func (u *ContentService) CreateStories(ctx context.Context, req *pb.CreateStoriesRequest) (*pb.CreateStoriesResponse, error) {
	u.Log.Info("CreateStories rpc method started")
	res, err := u.Repo.CreateStory(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("CreateStories rpc method finished")
	return res, nil
}
func (u *ContentService) UpdateStories(ctx context.Context, req *pb.UpdateStoriesReq) (*pb.UpdateStoriesRes, error) {
	u.Log.Info("UpdateStories rpc method started")
	res, err := u.Repo.UpdateStory(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("UpdateStories rpc method finished")
	return res, nil
}

func (u *ContentService) DeleteStories(ctx context.Context, req *pb.StoryId) (*pb.Void, error) {
	u.Log.Info("DeleteStories rpc method started")
	err := u.Repo.DeleteStory(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("DeleteStories rpc method finished")
	return &pb.Void{}, nil
}

func (u *ContentService) GetAllStories(ctx context.Context, req *pb.GetAllStoriesReq) (*pb.GetAllStoriesRes, error) {
	u.Log.Info("GetAllStories rpc method started")
	res, err := u.Repo.GetAllStory(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetAllStories rpc method finished")
	return res, nil
}

func (u *ContentService) GetStory(ctx context.Context, req *pb.StoryId) (*pb.GetStoryRes, error) {
	u.Log.Info("GetStory rpc method started")
	res, err := u.Repo.GetStoryById(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetStory rpc method finished")
	return res, nil
}

func (u *ContentService) CommentStory(ctx context.Context, req *pb.CommentStoryReq) (*pb.CommentStoryRes, error) {
	u.Log.Info("CommentStory rpc method started")
	res, err := u.Repo.CommentToStory(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("CommentStory rpc method finished")
	return res, nil
}

func (u *ContentService) GetCommentsOfStory(ctx context.Context, req *pb.GetCommentsOfStoryReq) (*pb.GetCommentsOfStoryRes, error) {
	u.Log.Info("GetCommentsOfStory rpc method started")
	res, err := u.Repo.GetCommentsOfStory(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetCommentsOfStory rpc method finished")
	return res, nil
}

func (u *ContentService) Like(ctx context.Context, req *pb.LikeReq) (*pb.LikeRes, error) {
	u.Log.Info("Like rpc method started")
	res, err := u.Repo.Like(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("Like rpc method finished")
	return res, nil
}

func (u *ContentService) Itineraries(ctx context.Context, req *pb.ItinerariesReq) (*pb.ItinerariesRes, error) {
	u.Log.Info("Itineraries rpc method started")
	res, err := u.Repo.Itineraries(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("Itineraries rpc method finished")
	return res, nil
}

func (u *ContentService) UpdateItineraries(ctx context.Context, req *pb.UpdateItinerariesReq) (*pb.ItinerariesRes, error) {
	u.Log.Info("UpdateItineraries rpc method started")
	res, err := u.Repo.UpdateItineraries(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("UpdateItineraries rpc method finished")
	return res, nil
}

func (u *ContentService) DeleteItineraries(ctx context.Context, req *pb.StoryId) (*pb.Void, error) {
	u.Log.Info("DeleteItineraries rpc method started")
	err := u.Repo.DeleteItineraries(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("DeleteItineraries rpc method finished")
	return &pb.Void{}, nil
}
func (u *ContentService) GetItineraries(ctx context.Context, req *pb.GetItinerariesReq) (*pb.GetItinerariesRes, error) {
	u.Log.Info("GetItineraries rpc method started")
	res, err := u.Repo.GetItineraries(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetItineraries rpc method finished")
	return res, nil
}
func (u *ContentService) GetItinerariesById(ctx context.Context, req *pb.StoryId) (*pb.GetItinerariesByIdRes, error) {
	u.Log.Info("GetItinerariesById rpc method started")
	res, err := u.Repo.GetItinerariesById(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetItinerariesById rpc method finished")
	return res, nil
}
func (u *ContentService) CommentItineraries(ctx context.Context, req *pb.CommentItinerariesReq) (*pb.CommentItinerariesRes, error) {
	u.Log.Info("CommentItineraries rpc method started")
	res, err := u.Repo.CommentItineraries(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("CommentItineraries rpc method finished")
	return res, nil
}
func (u *ContentService) GetDestinations(ctx context.Context, req *pb.GetDestinationsReq) (*pb.GetDestinationsRes, error) {
	u.Log.Info("GetDestinations rpc method started")
	res, err := u.Repo.GetDestinations(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetDestinations rpc method finished")
	return res, nil
}
func (u *ContentService) GetDestinationsById(ctx context.Context, req *pb.GetDestinationsByIdReq) (*pb.GetDestinationsByIdRes, error) {
	u.Log.Info("GetDestinationsById rpc method started")
	res, err := u.Repo.GetDestinationsById(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
	}
	u.Log.Info("GetDestinationsById rpc method finished")
	return res, nil
}
func (u *ContentService) SendMessage(ctx context.Context, req *pb.SendMessageReq) (*pb.SendMessageRes, error) {
	u.Log.Info("SendMessage rpc method started")
	res, err := u.Repo.SendMessage(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("SendMessage rpc method finished")
	return res, nil
}
func (u *ContentService) GetMessages(ctx context.Context, req *pb.GetMessagesReq) (*pb.GetMessagesRes, error) {
	u.Log.Info("GetMessages rpc method started")
	res, err := u.Repo.GetMessages(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetMessages rpc method finished")
	return res, nil
}
func (u *ContentService) CreateTips(ctx context.Context, req *pb.CreateTipsReq) (*pb.CreateTipsRes, error) {
	u.Log.Info("CreateTips rpc method started")
	res, err := u.Repo.CreateTips(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("CreateTips rpc method finished")
	return res, nil
}
func (u *ContentService) GetTips(ctx context.Context, req *pb.GetTipsReq) (*pb.GetTipsRes, error) {
	u.Log.Info("GetTips rpc method started")
	res, err := u.Repo.GetTips(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetTips rpc method finished")
	return res, nil
}
func (u *ContentService) GetUserStat(ctx context.Context, req *pb.GetUserStatReq) (*pb.GetUserStatRes, error) {
	u.Log.Info("GetUserStat rpc method started")
	res, err := u.Repo.GetUserStat(ctx, req)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("GetUserStat rpc method finished")
	return res, nil
}

func (u *ContentService) TopDestinations(ctx context.Context, req *pb.Void) (*pb.Answer, error) {
	u.Log.Info("TopDestinations rpc method started")
	res, err := redis.SaveTopDestinations(ctx, u.Repo)
	if err != nil {
		u.Log.Error(err.Error())
		return nil, err
	}
	u.Log.Info("TopDestinations rpc method finished")
	return res, nil
}
