package cwapi

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/streadway/amqp"
)

func (res *Response) UnmarshalJSON(b []byte) error {
	type alias Response
	temp := struct {
		Action  string          `json:"action"`
		Payload json.RawMessage `json:"payload"`
		*alias
	}{
		alias: (*alias)(res),
	}

	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}

	var payload resPayload
	if err := json.Unmarshal(temp.Payload, &payload); err != nil {
		return err
	}
	res.Payload.RequiredOperation = payload.RequiredOperation
	res.Payload.Token = payload.Token
	res.Action = temp.Action

	switch temp.Action {
	case "createAuthCode":
		var payload ResCreateAuthCode
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResCreateAuthCode = &payload
	case "grantToken":
		var payload ResGrantToken
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResGrantToken = &payload
	case "authAdditionalOperation":
		var payload ResAuthAdditionalOperation
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResAuthAdditionalOperation = &payload
	case "grantAdditionalOperation":
		var payload ResGrantAdditionalOperation
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResGrantAdditionalOperation = &payload
	case "authorizePayment":
		var payload ResAuthorizePayment
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResAuthorizePayment = &payload
	case "pay":
		var payload ResPay
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResPay = &payload
	case "payout":
		var payload ResPayout
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResPayout = &payload
	case "getInfo":
		var payload ResGetInfo
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResGetInfo = &payload
	case "viewCraftbook":
		var payload ResViewCraftbook
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResViewCraftbook = &payload
	case "requestProfile":
		var payload ResRequestProfile
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResRequestProfile = &payload
	case "requestBasicInfo":
		var payload ResRequestBasicInfo
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResRequestBasicInfo = &payload
	case "requestGearInfo":
		var payload ResRequestGearInfo
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResRequestGearInfo = &payload
	case "requestStock":
		var payload ResRequestStock
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResRequestStock = &payload
	case "guildInfo":
		var payload ResGuildInfo
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResGuildInfo = &payload
	case "wantToBuy":
		var payload ResWantToBuy
		if err := json.Unmarshal(temp.Payload, &payload); err != nil {
			return err
		}
		res.Payload.ResWantToBuy = &payload
	default:
		res.Action = "unknownMethod"
	}

	return nil
}

func (payload *reqPayload) MarshalJSON() ([]byte, error) {
	if payload.reqCreateAuthCode != nil {
		return json.Marshal(payload.reqCreateAuthCode)
	}
	if payload.reqGrantToken != nil {
		return json.Marshal(payload.reqGrantToken)
	}
	if payload.reqAuthAdditionalOperation != nil {
		return json.Marshal(payload.reqAuthAdditionalOperation)
	}
	if payload.reqGrantAdditionalOperation != nil {
		return json.Marshal(payload.reqGrantAdditionalOperation)
	}
	if payload.reqAuthorizePayment != nil {
		return json.Marshal(payload.reqAuthorizePayment)
	}
	if payload.reqPay != nil {
		return json.Marshal(payload.reqPay)
	}
	if payload.reqPayout != nil {
		return json.Marshal(payload.reqPayout)
	}
	if payload.reqWantToBuy != nil {
		return json.Marshal(payload.reqWantToBuy)
	}

	return json.Marshal(nil)
}

// Create new client, you can set server optional param, defaults to Chat Wars 2 server (or EU), accepts those variants:
// cw2, eu, cw3, ru
func NewClient(user string, password string, server ...string) (*Client, error) {
	rabbitUrl := fmt.Sprintf(CW2, user, password)
	var prefix string

	if len(server) > 0 {
		if strings.ToLower(server[0]) == "cw2" || strings.ToLower(server[0]) == "eu" {
			rabbitUrl = fmt.Sprintf(CW2, user, password)
			prefix = CW2PublicPrefix
		} else if strings.ToLower(server[0]) == "cw3" || strings.ToLower(server[0]) == "ru" {
			rabbitUrl = fmt.Sprintf(CW3, user, password)
			prefix = CW3PublicPrefix
		}
	}

	client := Client{
		User:         user,
		Password:     password,
		RabbitUrl:    rabbitUrl,
		PublicPrefix: prefix,
	}

	client.Updates = make(chan Response, 100)
	err := client.reconnect()
	return &client, err
}

func (c *Client) reStartConsumers() error {
	var err error

	if c.Updates != nil {
		err := c.startUpdateConsumer()
		if err != nil {
			return err
		}
	}

	if c.Deals != nil {
		err := c.InitDeals()
		if err != nil {
			return err
		}
	}

	if c.Duels != nil {
		err := c.InitDuels()
		if err != nil {
			return err
		}
	}

	if c.Offers != nil {
		err := c.InitOffers()
		if err != nil {
			return err
		}
	}

	if c.SexDigest != nil {
		err := c.InitSexDigest()
		if err != nil {
			return err
		}
	}

	if c.YellowPages != nil {
		err := c.InitYellowPages()
		if err != nil {
			return err
		}
	}

	if c.AuctionDigest != nil {
		err = c.InitAuctionDigest()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) reconnect() error {
	// Open new
	conn, err := amqp.Dial(c.RabbitUrl)
	if err != nil {
		return err
	}

	chForPublish, err := conn.Channel()
	if err != nil {
		return err
	}

	chForUpdates, err := conn.Channel()
	if err != nil {
		return err
	}

	// force close old channels and connection to close old consumers
	if c.channelForUpdates != nil {
		c.channelForUpdates.Close()
	}
	if c.channelForPublish != nil {
		c.channelForPublish.Close()
	}
	if c.connection != nil {
		c.connection.Close()
	}
	// Reassign it and restart consumers
	c.connection = conn
	c.channelForPublish = chForPublish
	c.channelForUpdates = chForUpdates

	err = c.reStartConsumers()
	if err != nil {
		return err
	}

	go func() {
		// Waits here for the channel to be closed
		log.Printf("closing: %s", <-c.connection.NotifyClose(make(chan *amqp.Error)))
		err = c.reconnect()
		if err != nil {
			log.Println("reconnect unsuccessful")
		} else {
			log.Println("reconnect successful")
		}
	}()

	return nil
}

// Start consumer for base events
func (c *Client) startUpdateConsumer() error {
	updates, err := c.channelForUpdates.Consume(
		fmt.Sprintf("%s_i", c.User),
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for update := range updates {
			if update.RoutingKey == fmt.Sprintf("%s_i", c.User) {
				var res Response
				err := json.Unmarshal(update.Body, &res)
				if err != nil {
					log.Println(err)
				}

				var userID int

				switch res.Action {
				case "createAuthCode":
					userID = res.Payload.ResCreateAuthCode.UserID
				case "grantToken":
					userID = res.Payload.ResGrantToken.UserID
				case "authAdditionalOperation":
					userID = res.Payload.ResAuthAdditionalOperation.UserID
				case "grantAdditionalOperation":
					userID = res.Payload.ResGrantAdditionalOperation.UserID
				case "authorizePayment":
					userID = res.Payload.ResAuthorizePayment.UserID
				case "pay":
					userID = res.Payload.ResPay.UserID
				case "payout":
					userID = res.Payload.ResPayout.UserID
				case "viewCraftbook":
					userID = res.Payload.ResViewCraftbook.UserID
				case "requestProfile":
					userID = res.Payload.ResRequestProfile.UserID
				case "requestBasicInfo":
					userID = res.Payload.ResRequestBasicInfo.UserID
				case "requestGearInfo":
					userID = res.Payload.ResRequestGearInfo.UserID
				case "requestStock":
					userID = res.Payload.ResRequestStock.UserID
				case "guildInfo":
					userID = res.Payload.ResGuildInfo.UserID
				case "wantToBuy":
					userID = res.Payload.ResWantToBuy.UserID
				}

				// trying to load update with this salt
				if waiter, found := c.waiters.Load(userID); found {
					// found? send it to waiter channel
					waiter.(chan Response) <- res

					// trying to prevent memory leak
					close(waiter.(chan Response))
				}

				c.Updates <- res
			}
		}
	}()
	return nil
}

// Close connection and active channel
func (c *Client) CloseConnection() error {
	close(c.Updates)
	close(c.Deals)
	close(c.Duels)
	close(c.Offers)
	close(c.SexDigest)
	close(c.YellowPages)
	close(c.AuctionDigest)

	if err := c.channelForUpdates.Close(); err != nil {
		return err
	}
	if err := c.channelForPublish.Close(); err != nil {
		return err
	}
	if err := c.connection.Close(); err != nil {
		return err
	}

	return nil
}

func (c *Client) makeRequest(req []byte) (err error) {
	err = c.channelForPublish.Publish(
		fmt.Sprintf("%s_ex", c.User),
		fmt.Sprintf("%s_o", c.User),
		true,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        req,
		},
	)
	// If channel closed
	if err != nil && err.(*amqp.Error).Code == 504 {
		err = c.reconnect()
		if err != nil {
			return err
		}
		// And try again
		if err := c.makeRequest(req); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
}
