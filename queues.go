package cwapi

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Initializes deals public exchange.
func (c *Client) InitDeals() error {
	c.Deals = make(chan Deal, 1)
	topics := []string{fmt.Sprintf("%sdeals", c.PublicPrefix)}
	pc, err := newPublicConsumer(topics)
	if err != nil {
		return err
	}
	go startConsuming(pc, c.dealHandler)
	return nil
}

func (c *Client) dealHandler(val []byte) error {
	var res Deal
	err := json.Unmarshal(val, &res)
	if err != nil {
		return fmt.Errorf("unmarshal %s error: %v", val, err)
	}
	c.Deals <- res
	return nil
}

// Initializes offers public exchange.
func (c *Client) InitDuels() error {
	c.Duels = make(chan Duel, 1)
	topics := []string{fmt.Sprintf("%sduels", c.PublicPrefix)}
	pc, err := newPublicConsumer(topics)
	if err != nil {
		return err
	}
	go startConsuming(pc, c.duelHandler)
	return nil
}

func (c *Client) duelHandler(val []byte) error {
	var res Duel
	err := json.Unmarshal(val, &res)
	if err != nil {
		return fmt.Errorf("unmarshal %s error: %v", val, err)
	}
	c.Duels <- res
	return nil
}

// Initializes offers public exchange.
func (c *Client) InitOffers() error {
	c.Offers = make(chan Offer, 1)
	topics := []string{fmt.Sprintf("%soffers", c.PublicPrefix)}
	pc, err := newPublicConsumer(topics)
	if err != nil {
		return err
	}
	go startConsuming(pc, c.offerHandler)
	return nil
}

func (c *Client) offerHandler(val []byte) error {
	var res Offer
	err := json.Unmarshal(val, &res)
	if err != nil {
		return fmt.Errorf("unmarshal %s error: %v", val, err)
	}
	c.Offers <- res
	return nil
}

// Initializes sex_digest public exchange.
func (c *Client) InitSexDigest() error {
	c.SexDigest = make(chan []SexDigestItem, 1)
	topics := []string{fmt.Sprintf("%ssex_digest", c.PublicPrefix)}
	pc, err := newPublicConsumer(topics)
	if err != nil {
		return err
	}
	go startConsuming(pc, c.sexDigestHandler)
	return nil
}

func (c *Client) sexDigestHandler(val []byte) error {
	var res []SexDigestItem
	err := json.Unmarshal(val, &res)
	if err != nil {
		return fmt.Errorf("unmarshal %s error: %v", val, err)
	}
	c.SexDigest <- res
	return nil
}

// Initializes yellow_pages public exchange.
func (c *Client) InitYellowPages() error {
	c.YellowPages = make(chan []YellowPage, 1)
	topics := []string{fmt.Sprintf("%syellow_pages", c.PublicPrefix)}
	pc, err := newPublicConsumer(topics)
	if err != nil {
		return err
	}
	go startConsuming(pc, c.yellowPagesHandler)
	return nil
}

func (c *Client) yellowPagesHandler(val []byte) error {
	var res []YellowPage
	err := json.Unmarshal(val, &res)
	if err != nil {
		return fmt.Errorf("unmarshal %s error: %v", val, err)
	}
	c.YellowPages <- res
	return nil
}

// Initializes au_digest public exchange.
func (c *Client) InitAuctionDigest() error {
	c.AuctionDigest = make(chan []AuctionDigestItem, 1)
	topics := []string{fmt.Sprintf("%sau_digest", c.PublicPrefix)}
	pc, err := newPublicConsumer(topics)
	if err != nil {
		return err
	}
	go startConsuming(pc, c.auctionDigestHandler)
	return nil
}

func (c *Client) auctionDigestHandler(val []byte) error {
	var res []AuctionDigestItem
	err := json.Unmarshal(val, &res)
	if err != nil {
		return fmt.Errorf("unmarshal %s error: %v", val, err)
	}
	c.AuctionDigest <- res
	return nil
}

func newPublicConsumer(topics []string) (*kafka.Consumer, error) {
	updates, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServer,
		// "broker.address.family":           "v4",
		"group.id":              kafkaGroupID,
		"enable.auto.commit":    true,
		"session.timeout.ms":    10000,
		"heartbeat.interval.ms": 3000,
		// "request.timeout.ms":       305000,
		"max.poll.interval.ms":     300000,
		"auto.offset.reset":        "latest",
		"go.events.channel.enable": true,
		"go.events.channel.size":   1,
		// "go.application.rebalance.enable": true,
		"enable.partition.eof": true,
	})
	if err != nil {
		return nil, err
	}
	err = updates.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}
	return updates, nil
}

func startConsuming(updates *kafka.Consumer, handler func([]byte) error) {
	defer updates.Close()

	for update := range updates.Events() {
		switch e := update.(type) {
		// case kafka.AssignedPartitions:
		// 	log.Printf("AssignedPartitions: %v\n", e)
		// 	if err := updates.Assign(e.Partitions); err != nil {
		// 		log.Printf("Error AssignedPartitions: %v\n", err)
		// 	}
		// case kafka.RevokedPartitions:
		// 	log.Printf("RevokedPartitions: %v\n", e)
		// 	if err := updates.Unassign(); err != nil {
		// 		log.Printf("Error RevokedPartitions: %v\n", err)
		// 	}
		case *kafka.Message:
			if err := handler(e.Value); err != nil {
				log.Printf("Error: %v\n", err)
				continue
			}
		case kafka.Error:
			log.Printf("Error in Kafka: %v\n", e)
		default:
			log.Printf("Ignored: %v\n", e)
		}
	}
}
