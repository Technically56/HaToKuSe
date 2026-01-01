package nodegrpcserver

import (
	"context"

	fm "github.com/Technically56/HaToKuSe/internal/node/storage/filemanager"
	pb "github.com/Technically56/HaToKuSe/proto/nodeservice"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NodeGrpcServer struct {
	pb.UnimplementedNodeServiceServer
	fm *fm.FileManager
}

func NewNodeGrpcServer(fileManager *fm.FileManager) *NodeGrpcServer {
	return &NodeGrpcServer{
		fm: fileManager,
	}
}
func (s *NodeGrpcServer) StoreFile(ctx context.Context, file *pb.File) (*pb.FileHash, error) {
	file_id := file.GetFileId()
	if _, err := uuid.Parse(file_id); err != nil {
		return nil, status.Error(codes.InvalidArgument, "file_id must be of uuid type")
	}
	content := file.GetFileContent()
	if len(content) == 0 {
		return nil, status.Error(codes.InvalidArgument, "file_content cannot be empty")
	}
	fileHash, err := s.fm.WriteToFile(file_id, content)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal write error")
	}
	return &pb.FileHash{FileId: file_id, FileHash: fileHash}, nil
}
func (s *NodeGrpcServer) GetFile(ctx context.Context, fileReq *pb.FileRequest) (*pb.File, error) {
	file_id := fileReq.GetFileId()
	if _, err := uuid.Parse(file_id); err != nil {
		return nil, status.Error(codes.InvalidArgument, "file_id must be of uuid type")
	}
	file_contents, err := s.fm.ReadFromFile(file_id)
	if err != nil {
		return nil, status.Error(codes.Internal, "internal read error")
	}
	return &pb.File{FileId: file_id, FileContent: file_contents}, nil
}

func (s *NodeGrpcServer) HasFile(ctx context.Context, fileReq *pb.FileRequest) (*pb.HasFileResponse, error) {
	file_id := fileReq.GetFileId()
	if _, err := uuid.Parse(file_id); err != nil {
		return nil, status.Error(codes.InvalidArgument, "file_id must be of uuid type")
	}
	hasFile := s.fm.HasFile(file_id)
	return &pb.HasFileResponse{HasFile: hasFile}, nil
}
func (s *NodeGrpcServer) GetFileCount(ctx context.Context, fileCountReq *pb.FileCountRequest) (*pb.FileCountResponse, error) {
	fileCount := s.fm.GetFileCounter()
	return &pb.FileCountResponse{FileCount: fileCount}, nil
}
