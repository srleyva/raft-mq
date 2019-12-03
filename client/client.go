package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/srleyva/raft-group-mq/pkg/message"
)

type clientAddr struct {
	addr   string
	client *grpc.ClientConn
}

func (c *clientAddr) Conn() *grpc.ClientConn {
	return c.client
}

func (c *clientAddr) String() string {
	return c.addr
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func main() {
	//names := []string{"Aaran", "Aaren", "Aarez", "Aarman", "Aaron", "Aaron-James", "Aarron", "Aaryan", "Aaryn", "Aayan", "Aazaan", "Abaan", "Abbas", "Abdallah", "Abdalroof", "Abdihakim", "Abdirahman", "Abdisalam", "Abdul", "Abdul-Aziz", "Abdulbasir", "Abdulkadir", "Abdulkarem", "Abdulkhader", "Abdullah", "Abdul-Majeed", "Abdulmalik", "Abdul-Rehman", "Abdur", "Abdurraheem", "Abdur-Rahman", "Abdur-Rehmaan", "Abel", "Abhinav", "Abhisumant", "Abid", "Abir", "Abraham", "Abu", "Abubakar", "Ace", "Adain", "Adam", "Adam-James", "Addison", "Addisson", "Adegbola", "Adegbolahan", "Aden", "Adenn", "Adie", "Adil", "Aditya", "Adnan", "Adrian", "Adrien", "Aedan", "Aedin", "Aedyn", "Aeron", "Afonso", "Ahmad", "Ahmed", "Ahmed-Aziz", "Ahoua", "Ahtasham", "Aiadan", "Aidan", "Aiden", "Aiden-Jack", "Aiden-Vee", "Aidian", "Aidy", "Ailin", "Aiman", "Ainsley", "Ainslie", "Airen", "Airidas", "Airlie", "AJ", "Ajay", "A-Jay", "Ajayraj", "Akan", "Akram", "Al", "Ala", "Alan", "Alanas", "Alasdair", "Alastair", "Alber", "Albert", "Albie", "Aldred", "Alec", "Aled", "Aleem", "Aleksandar", "Aleksander", "Aleksandr", "Aleksandrs", "Alekzander", "Alessandro", "Alessio", "Alex", "Alexander", "Alexei", "Alexx", "Alexzander", "Alf", "Alfee", "Alfie", "Alfred", "Alfy", "Alhaji", "Al-Hassan", "Ali", "Aliekber", "Alieu", "Alihaider", "Alisdair", "Alishan", "Alistair", "Alistar", "Alister", "Aliyaan", "Allan", "Allan-Laiton", "Allen", "Allesandro", "Allister", "Ally", "Alphonse", "Altyiab", "Alum", "Alvern", "Alvin", "Alyas", "Amaan", "Aman", "Amani", "Ambanimoh", "Ameer", "Amgad", "Ami", "Amin", "Amir", "Ammaar", "Ammar", "Ammer", "Amolpreet", "Amos", "Amrinder", "Amrit", "Amro", "Anay", "Andrea", "Andreas", "Andrei", "Andrejs", "Andrew", "Andy", "Anees", "Anesu", "Angel", "Angelo", "Angus", "Anir", "Anis", "Anish", "Anmolpreet", "Annan", "Anndra", "Anselm", "Danish", "Daniyal", "Danniel", "Danny", "Dante", "Danyal", "Danyil", "Danys", "Daood", "Dara", "Darach", "Daragh", "Darcy", "D'arcy", "Dareh", "Daren", "Darien", "Darius", "Darl", "Darn", "Darrach", "Darragh", "Darrel", "Darrell", "Darren", "Darrie", "Darrius", "Darroch", "Darryl", "Darryn", "Darwyn", "Daryl", "Daryn", "Daud", "Daumantas"}

	url := "https://raw.githubusercontent.com/dominictarr/random-name/master/names.json"
	httpclient := http.Client{
		Timeout: time.Second * 2, // Maximum of 2 secs
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("User-Agent", "mq-client")
	res, getErr := httpclient.Do(req)
	if getErr != nil {
		log.Fatal(getErr)
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Fatal(readErr)
	}
	names := []string{}
	jsonErr := json.Unmarshal(body, &names)
	if jsonErr != nil {
		log.Fatal(jsonErr)
	}

	log.Printf("Number of names: %d", len(names))

	addrs := []string{
		"localhost:12001",
		"localhost:13001",
		"localhost:14001",
		"localhost:15001",
	}

	clients := []consistent.Member{}

	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		client := &clientAddr{
			addr:   addr,
			client: conn,
		}
		clients = append(clients, client)
	}
	ring := consistent.New(clients, consistent.Config{
		PartitionCount:    181,
		ReplicationFactor: 8,
		Load:              1.2,
		Hasher:            hasher{},
	})

	msgs := []message.Message{
		message.Message{
			Topic: "sleyva",
			Event: "Yo",
		},
		message.Message{
			Topic: "jsweeney",
			Event: "Yo",
		},
		message.Message{
			Topic: "mwhite",
			Event: "Hey dude",
		},
		message.Message{
			Topic: "kcrawford",
			Event: "Kat!",
		},
		message.Message{
			Topic: "sleyva",
			Event: "How life",
		},
		message.Message{
			Topic: "sleyva",
			Event: "Alright",
		},
		message.Message{
			Topic: "sleyva",
			Event: "Its good",
		},
		message.Message{
			Topic: "sleyva",
			Event: "Hello",
		},
	}

	for i, name := range names {
		msgs = append(msgs, message.Message{Topic: name, Event: fmt.Sprintf("message %d", i)})
	}

	wg := &sync.WaitGroup{}
	for _, msg := range msgs {
		conn := ring.LocateKey([]byte(msg.Event)).(*clientAddr).Conn()
		client := message.NewMessageBusClient(conn)
		wg.Add(1)
		go newMessage(client, &msg, wg)
	}
	wg.Wait()

	conn := ring.LocateKey([]byte("sleyva")).(*clientAddr).Conn()
	client := message.NewMessageBusClient(conn)
	count := getMessages(client, "sleyva")
	for i := 0; i < count; i++ {
		fmt.Print("sleyva topic")
		proccessMessage(client, "sleyva")
	}

	newCount := getMessages(client, "mwhite")
	log.Print(newCount)

}

func proccessMessage(client message.MessageBusClient, topic string) {
	msg, err := client.ProccessMessage(context.Background(), &message.MessageRequest{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Topic: %s Msg: %s", topic, msg.Event)
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
		log.Printf("Topic: %s Message: %v", topic, msg)
		count++
	}
	log.Printf("There are %d messages", count)
	return count
}

func newMessage(client message.MessageBusClient, msg *message.Message, wg *sync.WaitGroup) {
	_, err := client.NewMessage(context.Background(), msg)
	if err != nil {
		log.Fatalf("Could not create message: %v", err)
	}
	wg.Done()
}
