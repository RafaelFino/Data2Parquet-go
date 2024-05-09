package server

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/handler"
	"data2parquet/pkg/receiver"
	"fmt"
	"log"
	"log/slog"

	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	engine *gin.Engine
	srv    *http.Server

	config   *config.Config
	handler  *handler.LogHandler
	receiver *receiver.Receiver
}

func NewServer(config *config.Config) *Server {
	s := &Server{
		engine:   gin.Default(),
		config:   config,
		receiver: receiver.NewReceiver(config),
	}

	slog.Debug("[server] Starting server", "config", config.ToString())

	s.handler = handler.NewLogHandler(config)

	gin.ForceConsoleColor()
	gin.DefaultWriter = log.Writer()
	gin.DefaultErrorWriter = log.Writer()

	if s.config.Debug {
		slog.Debug("[server] Debug mode enabled")
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	s.engine = gin.Default()
	s.engine.POST("/log/", s.handler.Write)
	s.engine.POST("/healthcheck/", s.handler.Healthcheck)

	s.srv = &http.Server{
		Addr:    s.makeAddress(),
		Handler: s.engine,
	}

	return s
}

func (s *Server) Run() {
	slog.Debug("[server] starting server", "address", s.makeAddress())
	err := s.srv.ListenAndServe()
	if err != nil {
		slog.Debug("[server] error starting server: %s", err)
		panic(err)
	}
}

func (s *Server) Stop() error {
	slog.Debug("[server] stopping receiver")
	err := s.receiver.Close()

	if err != nil {
		slog.Debug("[server] error stopping service", "error", err)
	}

	err = s.srv.Close()

	if err != nil {
		slog.Debug("[server] error stopping server", "error", err)
	}

	return err
}

func (s *Server) makeAddress() string {
	return fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
}
