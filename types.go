package cwapi

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	CW2 = "amqps://%s:%s@api.chatwars.me:5673/"
	CW3 = "amqps://%s:%s@api.chtwrs.com:5673/"

	kafkaServer     = "digest-api.chtwrs.com:9092"
	kafkaGroupID    = "some-super-duper-uniquie-id"
	CW2PublicPrefix = "cw2-"
	CW3PublicPrefix = "cw3-"
)

type ActionEnum string

const (
	CreateAuthCode           ActionEnum = "createAuthCode"
	GrantToken               ActionEnum = "grantToken"
	AuthAdditionalOperation  ActionEnum = "authAdditionalOperation"
	GrantAdditionalOperation ActionEnum = "grantAdditionalOperation"
	AuthorizePayment         ActionEnum = "authorizePayment"
	Pay                      ActionEnum = "pay"
	Payout                   ActionEnum = "payout"
	GetInfo                  ActionEnum = "getInfo"
	ViewCraftbook            ActionEnum = "viewCraftbook"
	RequestProfile           ActionEnum = "requestProfile"
	RequestBasicInfo         ActionEnum = "requestBasicInfo"
	RequestGearInfo          ActionEnum = "requestGearInfo"
	RequestStock             ActionEnum = "requestStock"
	GuildInfo                ActionEnum = "guildInfo"
	WantToBuy                ActionEnum = "wantToBuy"

	// Unknown action for this lib, check Chat Wars docs
	UnknownAction ActionEnum = "unknownAction"
)

// Returns constant with ActionEnum
func (res *Response) GetActionEnum() ActionEnum {
	switch res.Action {
	case "createAuthCode":
		return CreateAuthCode
	case "grantToken":
		return GrantToken
	case "authAdditionalOperation":
		return AuthAdditionalOperation
	case "grantAdditionalOperation":
		return GrantAdditionalOperation
	case "authorizePayment":
		return AuthorizePayment
	case "pay":
		return Pay
	case "payout":
		return Payout
	case "getInfo":
		return GetInfo
	case "viewCraftbook":
		return ViewCraftbook
	case "requestProfile":
		return RequestProfile
	case "requestBasicInfo":
		return RequestBasicInfo
	case "requestGearInfo":
		return RequestGearInfo
	case "requestStock":
		return RequestStock
	case "guildInfo":
		return GuildInfo
	case "wantToBuy":
		return WantToBuy
	default:
		return UnknownAction
	}
}

type ResultEnum string

const (
	// Everything is Ok
	Ok ResultEnum = "Ok"
	// Amount is either less than or equal zero
	BadAmount ResultEnum = "BadAmount"
	// The currency you chose is not allowed
	BadCurrency ResultEnum = "BadCurrency"
	// Message format is bad. It could be an invalid javascript, or types are wrong, or not all fields are sane
	BadFormat ResultEnum = "BadFormat"
	// The action you have requested is absent. Check spelling
	ActionNotFound ResultEnum = "ActionNotFound"
	// UserID is wrong, or user became inactive
	NoSuchUser ResultEnum = "NoSuchUser"
	// Your app is not yet registered
	NotRegistered ResultEnum = "NotRegistered"
	// Authorization code is incorrect
	InvalidCode ResultEnum = "InvalidCode"
	// Requested operation not exists
	NoSuchOperation ResultEnum = "NoSuchOperation"
	// If we have some technical difficulties, or bug and are willing for you to repeat request
	TryAgain ResultEnum = "TryAgain"
	// Some field of transaction is bad or confirmation code is wrong
	AuthorizationFailed ResultEnum = "AuthorizationFailed"
	// The player or application balance is insufficient
	InsufficientFunds ResultEnum = "InsufficientFunds"
	// The player is not a high enough level to do this action.
	LevelIsLow ResultEnum = "LevelIsLow"
	// The player is not in implied guild.
	NotInGuild ResultEnum = "NotInGuild"
	// No such token, might be revoked?
	InvalidToken ResultEnum = "InvalidToken"
	// Your app has no rights to execute this action with this token.
	// Payload will contain requiredOperation field.
	// We encourage you to use this field in following authAdditionalOperation, instead of enumerating existing ones
	Forbidden ResultEnum = "Forbidden"

	// Unknown result for this lib, check Chat Wars docs
	UnknownResult ResultEnum = "UnknownResult"
)

// Returns constant with ResultEnum
func (res *Response) GetResultEnum() ResultEnum {
	switch res.Result {
	case "Ok":
		return Ok
	case "BadAmount":
		return BadAmount
	case "BadCurrency":
		return BadCurrency
	case "BadFormat":
		return BadFormat
	case "ActionNotFound":
		return ActionNotFound
	case "NoSuchUser":
		return NoSuchUser
	case "NotRegistered":
		return NotRegistered
	case "InvalidCode":
		return InvalidCode
	case "NoSuchOperation":
		return NoSuchOperation
	case "TryAgain":
		return TryAgain
	case "AuthorizationFailed":
		return AuthorizationFailed
	case "InsufficientFunds":
		return InsufficientFunds
	case "LevelIsLow":
		return LevelIsLow
	case "NotInGuild":
		return NotInGuild
	case "InvalidToken":
		return InvalidToken
	case "Forbidden":
		return Forbidden
	default:
		return UnknownResult
	}
}

type Client struct {
	User         string
	Password     string
	Updates      chan Response
	RabbitUrl    string
	PublicPrefix string

	Deals         chan Deal
	Duels         chan Duel
	Offers        chan Offer
	SexDigest     chan []SexDigestItem
	YellowPages   chan []YellowPage
	AuctionDigest chan []AuctionDigestItem

	waiters           sync.Map
	connection        *amqp.Connection
	channelForUpdates *amqp.Channel
	channelForPublish *amqp.Channel
}

// Deals block
type Deal struct {
	SellerID     string `json:"sellerId"`
	SellerCastle string `json:"sellerCastle"`
	SellerName   string `json:"sellerName"`
	BuyerID      string `json:"buyerId"`
	BuyerCastle  string `json:"buyerCastle"`
	BuyerName    string `json:"buyerName"`
	Item         string `json:"item"`
	Quantity     int    `json:"qty"`
	Price        int    `json:"price"`
}

// Duels block
type Duelist struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Tag    string `json:"tag"`
	Castle string `json:"castle"`
	Level  int    `json:"level"`
	HP     int    `json:"hp"`
}

type Duel struct {
	Winner      *Duelist `json:"winner"`
	Loser       *Duelist `json:"loser"`
	IsChallenge bool     `json:"isChallenge"`
	IsGuildDuel bool     `json:"isGuildDuel"`
}

// Offers block
type Offer struct {
	SellerID     string `json:"sellerId"`
	SellerName   string `json:"sellerName"`
	SellerCastle string `json:"sellerCastle"`
	Item         string `json:"item"`
	Quantity     int    `json:"qty"`
	Price        int    `json:"price"`
}

// sex_digest block
type SexDigestItem struct {
	Name   string `json:"name"`
	Prices []int  `json:"prices"`
}

// yellow_pages block
type CraftSpecialization struct {
	Gloves int `json:"gloves,omitempty"`
	Coat   int `json:"coat,omitempty"`
	Helmet int `json:"helmet,omitempty"`
	Boots  int `json:"boots,omitempty"`
	Armor  int `json:"armor,omitempty"`
	Weapon int `json:"weapon,omitempty"`
	Shield int `json:"shield,omitempty"`
}
type CraftSpecializationsItem struct {
	Level  int                  `json:"Level,omitempty"`
	Values *CraftSpecialization `json:"Values,omitempty"`
}

type AlchemySpecialization struct {
	Apothecary    int `json:"apothecary,omitempty"`
	Dreamweaving  int `json:"dreamweaving,omitempty"`
	Blandleafurry int `json:"blandleafurry,omitempty"`
}

type AlchemySpecializationsItem struct {
	Level  int                    `json:"Level,omitempty"`
	Values *AlchemySpecialization `json:"Values,omitempty"`
}

type Specializations struct {
	QualityCraft *CraftSpecializationsItem   `json:"quality_craft,omitempty"`
	Alchemy      *AlchemySpecializationsItem `json:"alchemy,omitempty"`
}

type OfferItem struct {
	Item  string `json:"item"`
	Price int    `json:"price"`
	Mana  int    `json:"mana"`
}

type YellowPage struct {
	Link               string               `json:"link"`
	Name               string               `json:"name"`
	OwnerTag           string               `json:"ownerTag"`
	OwnerName          string               `json:"ownerName"`
	OwnerCastle        string               `json:"ownerCastle"`
	Kind               string               `json:"kind"`
	Mana               int                  `json:"mana"`
	Offers             []OfferItem          `json:"offers"`
	Specialization     *CraftSpecialization `json:"specialization,omitempty"`
	QualityCraftLevel  int                  `json:"qualityCraftLevel,omitempty"`
	Specializations    *Specializations     `json:"specializations,omitempty"`
	GuildDiscount      int                  `json:"guildDiscount,omitempty"`
	CastleDiscount     int                  `json:"castleDiscount,omitempty"`
	MaintenanceEnabled bool                 `json:"maintenanceEnabled,omitempty"`
	MaintenanceCost    int                  `json:"maintenanceCost,omitempty"`
}

type Request struct {
	Token   string          `json:"token"`
	Action  string          `json:"action"`
	Payload json.RawMessage `json:"payload"`
}

// au_digest block
type AuctionDigestItem struct {
	LotID        string         `json:"lotId"`
	ItemName     string         `json:"itemName"`
	Quality      string         `json:"quality"`
	Stats        map[string]int `json:"stats"`
	SellerName   string         `json:"sellerName"`
	SellerTag    string         `json:"sellerTag"`
	SellerCastle string         `json:"sellerCastle"`
	EndedAt      time.Time      `json:"endAt"`
	FinishedAt   time.Time      `json:"finishedAt"`
	Status       string         `json:"status"`
	StartedAt    time.Time      `json:"startedAt"`
	BuyerCastle  string         `json:"buyerCastle"`
	BuyerName    string         `json:"buyerName"`
	BuyerTag     string         `json:"buyerTag"`
	Price        int            `json:"price"`
}

type reqPayload struct {
	*reqCreateAuthCode
	*reqGrantToken
	*reqAuthAdditionalOperation
	*reqGrantAdditionalOperation
	*reqAuthorizePayment
	*reqPay
	*reqPayout
	*reqWantToBuy
}

type Response struct {
	UUID    string     `json:"uuid"`
	Action  string     `json:"action"`
	Result  string     `json:"result"`
	Payload resPayload `json:"payload"`
}

type resPayload struct {
	RequiredOperation string `json:"requiredOperation"`
	Token             string `json:"token"`
	*ResCreateAuthCode
	*ResGrantToken
	*ResAuthAdditionalOperation
	*ResGrantAdditionalOperation
	*ResAuthorizePayment
	*ResPay
	*ResPayout
	*ResGetInfo
	*ResViewCraftbook
	*ResRequestProfile
	*ResRequestBasicInfo
	*ResRequestGearInfo
	*ResRequestStock
	*ResGuildInfo
	*ResWantToBuy
}

type reqCreateAuthCode struct {
	UserID int `json:"userId"`
}

type ResCreateAuthCode struct {
	UserID int `json:"userId"`
}

type reqGrantToken struct {
	UserID   int    `json:"userId"`
	AuthCode string `json:"authCode"`
}

type ResGrantToken struct {
	UserID int    `json:"userId"`
	ID     string `json:"id"`
	Token  string `json:"token"`
}

type reqAuthAdditionalOperation struct {
	Operation string `json:"operation"`
}

type ResAuthAdditionalOperation struct {
	Operation string `json:"operation"`
	UserID    int    `json:"userId"`
}

type reqGrantAdditionalOperation struct {
	RequestID string `json:"requestId"`
	AuthCode  string `json:"authCode"`
}

type ResGrantAdditionalOperation struct {
	RequestID string `json:"requestId"`
	UserID    int    `json:"userId"`
}

type reqAuthorizePayment struct {
	TransactionID string         `json:"transactionId"`
	Amount        map[string]int `json:"amount"`
}

type ResAuthorizePayment struct {
	Fee           map[string]int `json:"fee"`
	Debit         map[string]int `json:"debit"`
	UserID        int            `json:"userId"`
	TransactionId string         `json:"transactionId"`
}

type reqPay struct {
	TransactionID    string         `json:"transactionId"`
	Amount           map[string]int `json:"amount"`
	ConfirmationCode string         `json:"confirmationCode"`
}

type ResPay struct {
	Fee           map[string]int `json:"fee"`
	Debit         map[string]int `json:"debit"`
	UserID        int            `json:"userId"`
	TransactionId string         `json:"transactionId"`
}

type reqPayout struct {
	TransactionID string         `json:"transactionId"`
	Amount        map[string]int `json:"amount"`
	Message       string         `json:"message"`
}

type ResPayout struct {
	UserID int `json:"userId"`
}

type ResGetInfo struct {
	Balance int `json:"balance"`
}

type CraftRecord struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
}

type ResViewCraftbook struct {
	Alchemy []*CraftRecord `json:"alchemy"`
	Craft   []*CraftRecord `json:"craft"`
	UserID  int            `json:"userId"`
}

type Profile struct {
	UserName   string `json:"userName"`
	Castle     string `json:"castle"`
	Level      int    `json:"lvl"`
	Experience int    `json:"exp"`
	Attack     int    `json:"atk"`
	Defense    int    `json:"def"`
	Gold       int    `json:"gold"`
	Pouches    int    `json:"pouches"`
	Guild      string `json:"guild"`
	GuildTag   string `json:"guild_tag"`
	Class      string `json:"class"`
	Mana       int    `json:"mana"`
	Stamina    int    `json:"stamina"`
	Health     int    `json:"hp"`
	MaxHealth  int    `json:"maxHp"`
}

type ResRequestProfile struct {
	Profile *Profile `json:"profile"`
	UserID  int      `json:"userId"`
}

type BasicProfile struct {
	Class   string `json:"class"`
	Attack  int    `json:"atk"`
	Defense int    `json:"def"`
}

type ResRequestBasicInfo struct {
	Profile *BasicProfile `json:"profile"`
	UserID  int           `json:"userId"`
}

type ResRequestGearInfo struct {
	Gear   map[string]string `json:"gear"`
	Ammo   map[string]int    `json:"ammo"`
	UserID int               `json:"userId"`
}

type ResRequestStock struct {
	Stock  map[string]int `json:"stock"`
	UserID int            `json:"userId"`
}

type ResGuildInfo struct {
	Tag        string         `json:"tag"`
	Level      int            `json:"level"`
	Castle     string         `json:"castle"`
	Glory      int            `json:"glory"`
	Members    int            `json:"members"`
	Name       string         `json:"name"`
	Lobby      string         `json:"lobby"`
	StockSize  int            `json:"stockSize"`
	StockLimit int            `json:"stockLimit"`
	Stock      map[string]int `json:"stock"`
	UserID     int            `json:"userId"`
}

type reqWantToBuy struct {
	ItemCode   string `json:"itemCode"`
	Quantity   int    `json:"quantity"`
	Price      int    `json:"price"`
	ExactPrice bool   `json:"exactPrice"`
}

type ResWantToBuy struct {
	ItemCode string `json:"itemCode"`
	Quantity int    `json:"quantity"`
	UserID   int    `json:"userId"`
}
