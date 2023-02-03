package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
	"log"
)

type Message struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")

	api.Post("/message", createMessage)
	app.Listen(":3000")
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushMessage(topic string, message []byte) error {
	brokersUrl := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(message),
		Partition: 3,
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Message is stored in topic(%s) - partition(%d) - offset(%d)\n", topic, partition, offset)
	return nil
}

func createMessage(ctx *fiber.Ctx) error {
	msg := new(Message)
	if err := ctx.BodyParser(msg); err != nil {
		log.Println(err)
		ctx.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	// convert body into bytes and send it to kafka
	mgsInBytes, _ := json.Marshal(msg)
	err := PushMessage("message", mgsInBytes)
	if err != nil {
		err = ctx.JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	// Return Comment in JSON format
	err = ctx.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": msg,
	})
	if err != nil {
		ctx.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error creating product",
		})
		return err
	}

	return err
}
