package main

import (
	"Content-Service/config"
	pb "Content-Service/genproto"
	"Content-Service/service"
	"Content-Service/storage/postgres"
	"log"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var logger *zap.Logger

func main() {
	cnf := config.Load()

	db, err := postgres.ConnectionDb()
	if err != nil {
		logger.Fatal("error connecting to database")
	}

	grpcServer := grpc.NewServer()
	pb.RegisterUserServiceServer(grpcServer, service.NewContentService(postgres.NewStoryRepository(db)))

	listen, err := net.Listen("tcp", cnf.HTTPPort)
	if err != nil {
		logger.Fatal("error setting up TCP listener")
	}

	log.Printf("Starting gRPC server on port %s...", cnf.HTTPPort)
	if err := grpcServer.Serve(listen); err != nil {
		logger.Fatal("error serving gRPC")
	}
}
