package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

// TelemetryData matches the structure of our streaming mock dataset
type TelemetryData struct {
	MachineID         string  `json:"machine_id" binding:"required"`
	CPUUtilization    float64 `json:"cpu_utilization" binding:"required"`
	MemoryUtilization float64 `json:"memory_utilization" binding:"required"`
	Status            string  `json:"status" binding:"required"`
	Timestamp         int64   `json:"timestamp" binding:"required"`
}

func main() {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	// Initialize the Pure-Go Kafka Writer (Producer)
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        "raw-telemetry",
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	r := gin.Default()

	// Define our ingestion endpoint
	r.POST("/telemetry", func(c *gin.Context) {
		var data TelemetryData

		// Validate incoming JSON structure
		if err := c.ShouldBindJSON(&data); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Convert struct to bytes for Kafka transmission
		payloadBytes, err := json.Marshal(data)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process payload"})
			return
		}

		// Push the message directly onto the Kafka conveyor belt
		err = writer.WriteMessages(context.Background(), kafka.Message{
			Value: payloadBytes,
		})

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue message: " + err.Error()})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{"status": "Data sent to queue successfully!"})
	})

	// Run our server on port 8080
	r.Run(":8080")
}
