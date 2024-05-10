package handler

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/receiver"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type LogHandler struct {
	rcv *receiver.Receiver
}

func NewLogHandler(config *config.Config) *LogHandler {
	return &LogHandler{
		rcv: receiver.NewReceiver(config),
	}
}

func (h *LogHandler) Write(ctx *gin.Context) {
	start := time.Now()

	slog.Debug("Write record", "module", "handler", "function", "Write")

	body, err := ctx.GetRawData()

	if err != nil {
		slog.Error("Error reading request body", "error", err, "module", "handler", "function", "Write")
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":     err.Error(),
			"timestamp": time.Now().Unix(),
			"elapsed":   time.Since(start).String(),
		})
		return
	}

	data := make(map[interface{}]interface{})

	err = json.Unmarshal([]byte(body), &data)

	if err != nil {
		slog.Debug("Error unmarshalling request body", "error", err, "module", "handler", "function", "Write")
		ctx.JSON(http.StatusBadRequest, gin.H{
			"error":     err.Error(),
			"timestamp": time.Now().Unix(),
			"elapsed":   time.Since(start).String(),
		})
		return
	}

	record := domain.NewLog(data)

	slog.Debug("Writing record", "record", record.ToString(), "module", "handler", "function", "Write")

	err = h.rcv.Write(record)

	if err != nil {
		slog.Error("Error writing record", "error", err, "module", "handler", "function", "Write")
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"record":    record.ToString(),
			"error":     err.Error(),
			"timestamp": time.Now().Unix(),
			"elapsed":   time.Since(start).String(),
		})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"timestamp": time.Now().Unix(),
		"elapsed":   time.Since(start).String(),
	})
}

func (h *LogHandler) Flush(ctx *gin.Context) {
	start := time.Now()

	slog.Debug("Flush buffer", "module", "handler", "function", "Flush")

	err := h.rcv.Flush(false)

	if err != nil {
		slog.Error("Error flushing buffer", "error", err, "module", "handler", "function", "Flush")
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"error":     err.Error(),
			"timestamp": time.Now().Unix(),
			"elapsed":   time.Since(start).String(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"timestamp": time.Now().Unix(),
		"elapsed":   time.Since(start).String(),
	})
}

func (h *LogHandler) Healthcheck(ctx *gin.Context) {
	start := time.Now()

	slog.Debug("Healthcheck", "module", "handler", "function", "Healthcheck")

	ctx.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"timestamp": time.Now().Unix(),
		"elapsed":   time.Since(start).String(),
	})
}
