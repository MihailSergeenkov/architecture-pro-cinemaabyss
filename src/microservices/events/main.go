package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// --- Schemas ---

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int     `json:"user_id"`
	Username  *string `json:"username,omitempty"`
	Email     *string `json:"email,omitempty"`
	Action    string  `json:"action"`
	Timestamp string  `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType *string `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type ErrorResp struct {
	Error string `json:"error"`
}

// --- Kafka producer/consumer ---

var producer sarama.SyncProducer

func kafkaProducer(brokers string) sarama.SyncProducer {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	prod, err := sarama.NewSyncProducer([]string{brokers}, cfg)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	return prod
}

// Consumer that writes messages to log
func consume(brokers, topic string) {
	cfg := sarama.NewConfig()
	client, err := sarama.NewConsumer([]string{brokers}, cfg)
	if err != nil {
		log.Printf("failed to start consumer: %v", err)
		return
	}
	partitions, _ := client.Partitions(topic)
	for _, p := range partitions {
		pc, err := client.ConsumePartition(topic, p, sarama.OffsetNewest)
		if err != nil {
			continue
		}
		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				log.Printf("Consumed: %s", string(msg.Value))
			}
		}(pc)
	}
}

// --- Handlers ---

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func postEventHandler(eventType string, mkPayload func([]byte) (interface{}, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := json.NewDecoder(r.Body)
		data, err := json.Marshal(body)
		if err != nil {
			http.Error(w, `{"error":"Bad request"}`, http.StatusBadRequest)
			return
		}
		payload, err := mkPayload(data)
		if err != nil {
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(ErrorResp{Error: "Invalid input"})
			return
		}
		id := eventType + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		now := time.Now().UTC().Format(time.RFC3339)
		ev := Event{
			ID:        id,
			Type:      eventType,
			Timestamp: now,
			Payload:   payload,
		}
		msgBytes, _ := json.Marshal(ev)
		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: eventType + "-events",
			Value: sarama.ByteEncoder(msgBytes),
		})
		if err != nil {
			log.Println(err.Error())
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(ErrorResp{Error: "Kafka error"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(EventResponse{
			Status:    "success",
			Partition: partition,
			Offset:    offset,
			Event:     ev,
		})
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:9092"
	}
	producer = kafkaProducer(brokers)
	go consume(brokers, "movie-events")
	go consume(brokers, "user-events")
	go consume(brokers, "payment-events")

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/api/events/health", healthHandler)
	r.Post("/api/events/movie", postEventHandler("movie", func(data []byte) (interface{}, error) {
		var m MovieEvent
		return m, json.Unmarshal(data, &m)
	}))
	r.Post("/api/events/user", postEventHandler("user", func(data []byte) (interface{}, error) {
		var u UserEvent
		return u, json.Unmarshal(data, &u)
	}))
	r.Post("/api/events/payment", postEventHandler("payment", func(data []byte) (interface{}, error) {
		var p PaymentEvent
		return p, json.Unmarshal(data, &p)
	}))
	log.Printf("Listening on :%s", port)
	http.ListenAndServe(":"+port, r)
}
