package main

import (
	"context"
	"io"
	"log"

	"google.golang.org/grpc"

	"github.com/srleyva/raft-group-mq/pkg/message"
)

func main() {
	addr := "localhost:12001"
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := message.NewMessageBusClient(conn)
	msgs := []message.Message{
		message.Message{
			Topic: "sleyva-topic-1",
			Event: "Hello",
		},
		message.Message{
			Topic: "sleyva-topic-2",
			Event: "I",
		},
		message.Message{
			Topic: "sleyva-topic-1",
			Event: "Am",
		},
		message.Message{
			Topic: "sleyva-topic-2",
			Event: "A",
		},
		message.Message{
			Topic: "sleyva-topic-1",
			Event: "Message",
		},
		message.Message{
			Topic: "sleyva-topic-2",
			Event: "From",
		},
		message.Message{
			Topic: "sleyva-topic-1",
			Event: "A",
		},
		message.Message{
			Topic: "sleyva-topic-2",
			Event: "Client",
		},
	}
	for _, msg := range msgs {
		newMessage(client, &msg)
	}
	count := getMessages(client, "sleyva-topic-1")
	for i := 0; i < count; i++ {
		proccessMessage(client)
	}

	newCount := getMessages(client, "sleyva-topic-2")
	log.Print(newCount)

}

func proccessMessage(client message.MessageBusClient) {
	msg, err := client.ProccessMessage(context.Background(), &message.MessageRequest{
		Topic: "sleyva-topic-1",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Print(msg.Event)
}

func getMessages(client message.MessageBusClient, topic string) int {
	// count messages
	count := 0
	// calling the streaming API
	stream, err := client.ListMessages(context.Background(), &message.MessageRequest{
		Topic: topic,
	})
	if err != nil {
		log.Fatalf("Error on get messages: %v", err)
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%v.ListMessages(_) = _, %v", client, err)
		}
		log.Printf("Message: %v", msg)
		count++
	}
	log.Printf("There are %d messages", count)
	return count
}

func newMessage(client message.MessageBusClient, msg *message.Message) {
	_, err := client.NewMessage(context.Background(), msg)
	if err != nil {
		log.Fatalf("Could not create message: %v", err)
	}
}
