package rabbit

import (
	"fmt"
	"github.com/getCompassUtils/go_base_frame/api/system/functions"
	"github.com/getCompassUtils/go_base_frame/api/system/log"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

const (
	routinesMax                  = 50  // сколько рутин может одновременно обрабатывать сообщения из очереди
	messagesToConsumerPerRequest = 100 // сколько сообщений брать за раз на выполнение (глубина продавливания)
	exchangeType                 = "fanout"
)

// структура соединения
type ConnectionStruct struct {
	connection *amqp.Connection // соединение
	key        string
	channel    *amqp.Channel    // канал
	errorChan  chan *amqp.Error // канал ошибок
	createdAt  int64
}

// структура соединения
type rabbitQueuesStorage struct {
	queueMap map[string]bool
	mu       sync.Mutex
}

// хранилище для очередей rabbit
var rabbitQueuesStore = rabbitQueuesStorage{queueMap: make(map[string]bool)}

// создаем буферизированный канал, который определяет максимальное количество запущенных рутин
var guardChan = make(chan struct{}, routinesMax)

// структура соединения
type rabbitConnectionStorage struct {
	connectionMap map[string]*ConnectionStruct
	mu            sync.Mutex
}

// объявляем хранилище
var rabbitConnectionStore = rabbitConnectionStorage{connectionMap: make(map[string]*ConnectionStruct)}

// -------------------------------------------------------
// PUBLIC
// -------------------------------------------------------

// слушаем
func (connectionItem *ConnectionStruct) Listen(queueName string, exchangeName string, callback func(body []byte) []byte) {

	// слушаем соединение
	listenConnection(connectionItem, queueName, exchangeName, callback)
}

// отправляем сообщение в очередь
func (connectionItem *ConnectionStruct) SendMessageToQueue(queueName string, message []byte) {

	// формируем объект для отпрвки
	publishingItem := amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now(),
		ContentType:  "shortstr",
	}

	// подставляем сообщение
	publishingItem.Body = message

	// добавляем в очередь
	connectionItem.declareQueueIfNeed(queueName)
	err := connectionItem.channel.Publish("", queueName, false, false, publishingItem)
	if err != nil {
		log.Errorf("unable publish message to %s rabbitMq, error: %v", queueName, err)
	}
}

// SendMessageListToQueue отправляем сообщения в очередь
func (connectionItem *ConnectionStruct) SendMessageListToQueue(queueName string, messageList [][]byte) {

	// формируем объект для отпрвки
	publishingItem := amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now(),
		ContentType:  "shortstr",
	}

	// проходимся по каждому сообщению
	for _, message := range messageList {

		// подставляем сообщение
		publishingItem.Body = message

		// добавляем в очередь
		connectionItem.declareQueueIfNeed(queueName)
		err := connectionItem.channel.Publish("", queueName, false, false, publishingItem)
		if err != nil {
			log.Errorf("unable publish message to %s rabbitMq, error: %v", queueName, err)
		}
	}
}

// отправляем сообщения в очередь
func (connectionItem *ConnectionStruct) SendMessageListToExchange(exchangeName string, messageList [][]byte) {

	// формируем объект для отпрвки
	publishingItem := amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now(),
		ContentType:  "shortstr",
	}

	// проходимся по каждому сообщению
	for _, message := range messageList {

		// подставляем сообщение
		publishingItem.Body = message

		// добавляем в очередь
		err := connectionItem.channel.Publish(exchangeName, "", false, false, publishingItem)
		if err != nil {
			log.Errorf("unable publish message to %s rabbitMq, error: %v", exchangeName, err)
		}
	}
}

// закрываем все соединения
func (connectionItem *ConnectionStruct) CloseAll() {

	_ = connectionItem.channel.Close()
	_ = connectionItem.connection.Close()
}

// -------------------------------------------------------
// PROTECTED
// -------------------------------------------------------

// слушаем соединение
func listenConnection(connectionItem *ConnectionStruct, queueName string, exchangeName string, callback func(body []byte) []byte) {

	connectionItem.declareQueueIfNeed(queueName)
	if exchangeName != "" {
		connectionItem.bindQueueToExchange(queueName, exchangeName)
	}

	eventChan, err := connectionItem.channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {

		panic(fmt.Errorf("unable get eventChan for connection, error: %v", err))
	}

	go listenChannel(eventChan, callback)

	for {

		err = <-connectionItem.errorChan
		if err != nil {
			panic(err)
		}
	}
}

// слушаем канал
func listenChannel(eventChan <-chan amqp.Delivery, callback func(body []byte) []byte) {

	// логируем начало прослушивания
	log.Success("start listening rabbitMq")

	// слушаем канал
	for event := range eventChan {

		// обрабатываем запрос
		guardChan <- struct{}{}
		go handleRequest(event, callback)
	}
}

// обрабатываем запрос
func handleRequest(event amqp.Delivery, callback func(body []byte) []byte) {

	// помечаем событие обработанным
	defer func() {

		_ = event.Ack(false)
		<-guardChan
	}()

	// логируем новый запрос
	log.Infof("request from rabbitMq, received message: %s", string(event.Body))

	callback(event.Body)
}

// получить соединение rabbit
func GetRabbitConnection(key string) (*ConnectionStruct, bool) {

	// блокируем хранилище
	rabbitConnectionStore.mu.Lock()

	// разблокируем хранилище
	defer rabbitConnectionStore.mu.Unlock()

	rabbitConnect, isExist := rabbitConnectionStore.connectionMap[key]

	return rabbitConnect, isExist
}

// получаем конфигурацию main
func UpdateRabbitConnection(key string, connect *ConnectionStruct) {

	// блокируем хранилище
	rabbitConnectionStore.mu.Lock()

	// разблокируем хранилище
	defer rabbitConnectionStore.mu.Unlock()

	rabbitConnectionStore.connectionMap[key] = connect
}

// создаем объект соединения
func OpenRabbitConnection(key string, user string, pass string, host string, port string) (*ConnectionStruct, error) {

	// устанавливаем соединение
	connection, err := getConnection(user, pass, host, port)
	if err != nil {

		log.Errorf("unable connect to rabbitMq, error: %v", err)
		return nil, err
	}

	// создаем объект соединения
	connectionItem := ConnectionStruct{
		connection: connection,
		key:        key,
		channel:    getChannel(connection),
		errorChan:  make(chan *amqp.Error),
		createdAt:  functions.GetCurrentTimeStamp(),
	}

	// указываем канал, куда будем отправлять ошибки о потере соединения с rabbitMq
	connection.NotifyClose(connectionItem.errorChan)

	return &connectionItem, nil
}

// устанавливаем соединение
func getConnection(user string, pass string, host string, port string) (*amqp.Connection, error) {

	// генеририруем ссылку
	rabbitUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pass, host, port)

	// поключаемся к rabbitMq
	connection, err := amqp.Dial(rabbitUrl)
	if err != nil {
		return nil, err
	}

	return connection, nil
}

// получаем канал
func getChannel(connection *amqp.Connection) *amqp.Channel {

	// открываем канал
	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}

	// устанавливаем сколько сообщений приходит за раз
	_ = channel.Qos(messagesToConsumerPerRequest, 0, false)

	return channel
}

// биндим эксчендж к очереди
func (connectionItem *ConnectionStruct) bindQueueToExchange(queueName string, exchange string) {

	connectionItem.declareQueueIfNeed(queueName)

	// устанавливаем связь нашей очереди с обменником
	err := connectionItem.channel.ExchangeDeclare(exchange, exchangeType, false, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	err = connectionItem.channel.QueueBind(queueName, "", exchange, false, nil)
	if err != nil {
		panic(err)
	}
}

// создаем очередь если ее нет
func (connectionItem *ConnectionStruct) declareQueueIfNeed(queueName string) {

	// блокируем хранилище
	rabbitQueuesStore.mu.Lock()

	// разблокируем хранилище
	defer rabbitQueuesStore.mu.Unlock()

	// получаем соединение из хранилища
	_, isExist := rabbitQueuesStore.queueMap[queueName]

	if !isExist {

		// определяем очередь
		_, err := connectionItem.channel.QueueDeclare(queueName, false, false, false, false, nil)
		if err != nil {
			panic(err)
		}

		rabbitQueuesStore.queueMap[queueName] = true
	}
}
