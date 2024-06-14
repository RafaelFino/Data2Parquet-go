package server

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/handler"
	"data2parquet/pkg/logger" // "log/slog"
	"data2parquet/pkg/receiver"
	"fmt"
	"os"

	"net/http"

	"github.com/gin-gonic/gin"
)

var slog = logger.GetLogger()

type Server struct {
	engine *gin.Engine
	srv    *http.Server
	ctx    context.Context

	config   *config.Config
	handler  *handler.LogHandler
	receiver *receiver.Receiver
}

func NewServer(ctx context.Context, config *config.Config) *Server {
	s := &Server{
		engine:   gin.Default(),
		config:   config,
		receiver: receiver.NewReceiver(ctx, config),
		ctx:      ctx,
	}

	slog.Debug("Starting server", "config", config.ToString(), "module", "server", "function", "NewServer")

	s.handler = handler.NewRecordHandler(ctx, config)

	gin.ForceConsoleColor()
	gin.DefaultWriter = os.Stdout
	gin.DefaultErrorWriter = os.Stderr

	if s.config.Debug {
		slog.Debug("Debug mode enabled", "module", "server", "function", "NewServer")
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	s.engine = gin.Default()
	s.engine.POST("/record/", s.handler.Write)
	s.engine.POST("/flush/", s.handler.Flush)
	s.engine.GET("/healthcheck/", s.handler.Healthcheck)

	s.srv = &http.Server{
		Addr:    s.makeAddress(),
		Handler: s.engine,
	}

	return s
}

func (s *Server) Run() error {
	slog.Debug("Starting server", "address", s.makeAddress(), "module", "server", "function", "Run")
	err := s.srv.ListenAndServe()
	if err != nil {
		slog.Error("Error on server", "error", err, "module", "server", "function", "Run")
	}

	return err
}

func (s *Server) Stop() error {
	slog.Debug("[Stopping receiver", "module", "server", "function", "Stop")
	err := s.receiver.Close()

	if err != nil {
		slog.Debug("Error stopping service", "error", err, "module", "server", "function", "Stop")
	}

	err = s.srv.Close()

	if err != nil {
		slog.Debug("Error stopping server", "error", err, "module", "server", "function", "Stop")
	}

	return err
}

func (s *Server) makeAddress() string {
	return fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
}
