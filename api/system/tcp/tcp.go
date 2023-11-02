package tcp

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/getCompassUtils/go_base_frame"
	"github.com/getCompassUtils/go_base_frame/api/system/functions"
	"github.com/getCompassUtils/go_base_frame/api/system/log"
	"net"
	"time"
)

// -------------------------------------------------------
// пакет для обработки сообщений через TCP соединение
// -------------------------------------------------------

const (

	// максимальное количество соединений
	routinesMax = 1000

	// тип содеинения
	connectionType = "tcp4"

	// таймаут tcp соединений
	connectionTimeout = time.Second * 2
)

// структура соединения
type connectionStruct struct {
	conn      *net.TCPConn   // соединение
	scanner   *bufio.Scanner // сканнер
	startTime int64          // время установки соединения
}

// создаем буферизированный канал, который определяет максимальное количество запущенных рутин
var guardChan = make(chan struct{}, routinesMax)

// -------------------------------------------------------
// PUBLIC
// -------------------------------------------------------

// слушаем tcp соединение
func Listen(host string, port int64, callback func(body []byte) []byte) {

	// начинаем слушать порт
	listener, err := net.Listen(connectionType, fmt.Sprintf("%s:%d", host, port))
	if err != nil {

		log.Errorf("unable to start listening, error: %v", err)
		return
	}

	// объявляем дефер
	defer func() { _ = listener.Close() }()

	// логируем успешное начало прослушивания
	log.Successf("start listening tcp on %s:%d", host, port)

	// слушаем входящие соедиения
	_listenConnections(listener, callback)
}

// слушаем входящие соедиения
func _listenConnections(listener net.Listener, callback func(body []byte) []byte) {

	// слушаем в бесконечном цикле
	for {

		// принимаем новые соединения
		conn, err := listener.Accept()
		if err != nil {

			log.Errorf("unable to accept request, error: %v", err)
			return
		}

		// создаем объект tcp соединения
		connectionItem := _makeConnectionItem(conn)

		// слушаем соединение в отдельной рутине
		guardChan <- struct{}{}
		go _listenConnection(connectionItem, callback)
	}
}

// создаем объект соединения
func _makeConnectionItem(conn net.Conn) connectionStruct {

	// определяем соединение как tcp
	tcpConn := conn.(*net.TCPConn)

	// устанавливаем таймаут
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(connectionTimeout)

	// увеличиваем замер буффера, куда сложим входящий запрос
	scanner := bufio.NewScanner(tcpConn)
	var buf []byte
	scanner.Buffer(buf, 512000)

	// возвращаем новый объект
	return connectionStruct{
		conn:      tcpConn,
		scanner:   scanner,
		startTime: functions.GetCurrentTimeStamp(),
	}
}

// слушаем соединение
func _listenConnection(connectionItem connectionStruct, callback func(body []byte) []byte) {

	// закрываем соединение по завершению работы функции
	defer func() {

		_ = connectionItem.conn.Close()
		<-guardChan
	}()

	// слушаем соединение
	for connectionItem.scanner.Scan() {

		// получаем сообщение из соединения
		message, isSuccess := _getMessage(connectionItem)
		if !isSuccess {
			continue
		}

		// обрабатываем запрос
		_handleRequest(message, connectionItem, callback)
	}
}

// получаем сообщение из соединения
func _getMessage(connectionItem connectionStruct) ([]byte, bool) {

	// получаем текст сообщения в формате memcache
	message := connectionItem.scanner.Text()

	// опрделяем длину сообщения
	messageLength := len(message)

	// проверяем что сообщение корректно
	if len(message) < 4 {
		return []byte{}, false
	}

	// убираем "get " в начале, чтобы получить чистый json
	return []byte(message[4:messageLength]), true
}

// обрабатываем запрос
func _handleRequest(message []byte, connectionItem connectionStruct, callback func(body []byte) []byte) {

	// логируем новый запрос
	log.Infof("connection started at: %d, received message: %s", connectionItem.startTime, string(message))

	// выполняем запрос
	result := callback(message)
	response := []byte(fmt.Sprintf("VALUE %s 0 %d\r\n%s\r\nEND\r\n", "request", len(string(result)), result))

	// логируем ответ
	log.Infof("answering request with: %s", response)

	_, _ = connectionItem.conn.Write(response)
}

// метод для отправки сообщения в tcp соединение
func DoSendRequest(host string, port string, request []byte) (interface{}, error) {

	// отправляем tcp запрос
	response, err := _doTcpRequest(host, port, request)
	if err != nil {
		return nil, fmt.Errorf("unable send tcp request, error: %v", err)
	}

	return response, err
}

// метод отправляет request по tcp соединению
func _doTcpRequest(host string, port string, request []byte) (interface{}, error) {

	conn, err := _getConnection(host, port)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	requestBytes := _getFormattedRequest(request)
	_, err = conn.Write(requestBytes)
	if err != nil {
		return nil, err
	}

	var response interface{}
	reader := bufio.NewReader(conn)
	err = _getResponse(reader, response)
	return response, err
}

// получаем отформатированный запрос
func _getFormattedRequest(request []byte) []byte {

	return []byte(fmt.Sprintf("get %s\r\n", request))

}

// функция для получения соединения
func _getConnection(host string, port string) (*net.TCPConn, error) {

	tcpAddr, err := net.ResolveTCPAddr(connectionType, fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP(connectionType, nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// метод для разбора ответа по tcp
func _getResponse(r *bufio.Reader, response interface{}) error {

	var responseBytes = make([]byte, 32768)

	_, err := r.Read(responseBytes)
	if err != nil {
		return fmt.Errorf("Не смогли прочитать response с запроса\r\nError: %v", err)
	}

	responseSlice := bytes.Split(responseBytes, []byte("\r\n"))
	if len(responseSlice) < 2 {
		return fmt.Errorf("Incorrect request from microservice ")
	}

	err = go_base_frame.Json.Unmarshal(responseSlice[1], &response)
	if err != nil {
		return err
	}

	return nil
}
