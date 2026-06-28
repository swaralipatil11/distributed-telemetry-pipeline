package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

	// Define our health check endpoint
	r.GET("/healthz", func(c *gin.Context) {
		// Attempt a quick TCP connection to Kafka broker to check health
		conn, err := net.DialTimeout("tcp", kafkaBroker, 2*time.Second)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "unhealthy",
				"error":  "Failed to connect to Kafka Broker: " + err.Error(),
			})
			return
		}
		conn.Close()

		c.JSON(http.StatusOK, gin.H{
			"status": "healthy",
		})
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Start server in a background goroutine so that it doesn't block shutdown detection
	go func() {
		log.Println("[INFO] Starting Go Ingestion API Gateway on port 8080...")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] Listen error: %s\n", err)
		}
	}()

	// Channel to listen for OS termination signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	
	// Block until a signal is received
	sig := <-quit
	log.Printf("[INFO] Received signal %v. Initiating graceful server shutdown...\n", sig)

	// Set a 5-second timeout for remaining active connections to drain
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[FATAL] API Server forced to shutdown: %v", err)
	}

	log.Println("[SUCCESS] Go Ingestion API Gateway clean shutdown complete.")
}
