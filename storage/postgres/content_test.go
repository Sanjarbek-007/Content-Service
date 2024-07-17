package postgres

import (
	pb "Content-Service/genproto/content"
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestCreateStory(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	con := NewContentRepository(db)
	req, err := con.CreateStory(context.Background(), &pb.CreateStoriesRequest{
		Title:    "new",
		Content:  "new",
		Location: "new",
		Tags:     []string{"new1", "new2", "new3"},
		UserId:   "0d39904b-05ce-4a9b-bd72-f6f4d66c1ba1",
	})
	if err != nil {
		fmt.Println(err)
	}
	res := pb.CreateStoriesResponse{
		Id:        "3f9e0c08-323f-4fdf-868e-7bfbd092dabe",
		Title:     "new",
		Content:   "new",
		Location:  "new",
		Tags:      []string{"new1", "new2", "new3"},
		AuthorId:  "0d39904b-05ce-4a9b-bd72-f6f4d66c1ba1",
		CreatedAt: "2024-07-15T22:24:19.319013+05:00",
	}

	fmt.Println(req.CreatedAt, req.Id)

	if req.Title != res.Title || req.Content != res.Content || req.Location != res.Location || req.AuthorId != res.AuthorId {
		t.Errorf("CreateStoriesRequest returned %+v, want %+v", req, &res)
	}
}

func TestUpdateStory(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	con := NewContentRepository(db)
	req, err := con.UpdateStory(context.Background(), &pb.UpdateStoriesReq{
		Id:      "3f9e0c08-323f-4fdf-868e-7bfbd092dabe",
		Title:   "old",
		Content: "old",
	})
	if err != nil {
		fmt.Println(err)
	}
	res := pb.UpdateStoriesRes{
		Id:        "3f9e0c08-323f-4fdf-868e-7bfbd092dabe",
		Title:     "old",
		Content:   "old",
		Location:  "new",
		Tags:      []string{"new1", "new2", "new3"},
		AuthorId:  "0d39904b-05ce-4a9b-bd72-f6f4d66c1ba1",
		UpdatedAt: "2024-07-15T13:38:16.471113+05:00",
	}

	if !reflect.DeepEqual(req, &res) {
		t.Errorf("UpdateStoriesRequest returned %+v, want %+v", req, &res)
	}
}

func TestDeleteStory(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	con := NewContentRepository(db)
	err = con.DeleteStory(context.Background(), &pb.StoryId{
		Id: "3f9e0c08-323f-4fdf-868e-7bfbd092dabe",
	})
	if err != nil {
		fmt.Println(err)
	}
}

func TestGetAllStories(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	con := NewContentRepository(db)
	res, err := con.GetAllStory(context.Background(), &pb.GetAllStoriesReq{
		Limit: 10,
	})
	fmt.Println(res)
	if err != nil {
		fmt.Println(err)
	}
}

func TestGetStory(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	con := NewContentRepository(db)
	res, err := con.GetStoryById(context.Background(), &pb.StoryId{Id: "050a8f3b-a3e5-4ed1-abe4-cdf030977788"})
	fmt.Println(res)
	if err != nil {
		fmt.Println(err)
	}
	req := &pb.GetStoryRes{
		Id:       "050a8f3b-a3e5-4ed1-abe4-cdf030977788",
		Title:    "old",
		Content:  "old",
		Location: "new",
		Tags:     []string{"new1", "new2", "new3"},
		Author: &pb.Author{
			UserId:   "45e65723-0781-4e7d-bb7f-e87efddda823",
			Username: "user1",
			FullName: "User One",
		},
		LikesCount:    0,
		CommentsCount: 0,
		CreatedAt:     "2024-07-15T13:38:16.471113+05:00",
		UpdatedAt:     "2024-07-15T13:44:13.568335+05:00",
	}
	if !reflect.DeepEqual(req, res) {
		t.Errorf("GetStoryRequest returned %+v, want %+v", req, res)
	}
}

func TestCommentToStory(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	con := NewContentRepository(db)
	res, err := con.CommentToStory(context.Background(), &pb.CommentStoryReq{
		StoryId:  "3f9e0c08-323f-4fdf-868e-7bfbd092dabe",
		Content:  "zor",
		AuthorId: "0d39904b-05ce-4a9b-bd72-f6f4d66c1ba1",
	})
	if err != nil {
		fmt.Println(err)
	}
	req := &pb.CommentStoryRes{
		Id:       "49345363-c816-4939-b4d9-46e40a221e55",
		StoryId:  "3f9e0c08-323f-4fdf-868e-7bfbd092dabe",
		Content:  "zor",
		AuthorId: "45e65723-0781-4e7d-bb7f-e87efddda823",
	}
	if !reflect.DeepEqual(req, res) {
		t.Errorf("CommentToStoryRequest returned %+v, want %+v", req, res)
	}
}

func TestGetCommentsOfStory(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	con := NewContentRepository(db)
	res, err := con.GetCommentsOfStory(context.Background(), &pb.GetCommentsOfStoryReq{StoryId: "3f9e0c08-323f-4fdf-868e-7bfbd092dabe", Limit: 10})
	fmt.Println(res)
	if err != nil {
		fmt.Println(err)
	}

}
