package tcp

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
)

// -------------------------------------------------------
// пакет для осуществления TCP запросов в тестах
// -------------------------------------------------------

// доступное кол-во реконнектов для соединения
const maxReconnectCount = 3

// пул соединений
type tcpConnectionStorage struct {
	mu    sync.Mutex
	cache map[int64]*tcpConnectionStruct
}

// структра TCP соединения
type tcpConnectionStruct struct {
	connectionItem *net.TCPConn
	readerItem     *bufio.Reader
	tcpPort        int64
	reconnectCount int64
}

// инициализируем пул соединений
var tcpConnectionStore = tcpConnectionStorage{
	cache: make(map[int64]*tcpConnectionStruct),
}

// -------------------------------------------------------
// PUBLIC
// -------------------------------------------------------

// обращаемся по TCP и возвращаем ответ
func Call(requestData []byte, tcpPort ...int64) ([]byte, error) {

	// получаем TCP соединение
	tcpConnectionItem, err := tcpConnectionStore.getTcpConnectionItem(getTcpPort(tcpPort))
	if err != nil {
		return []byte{}, err
	}

	// отправляем запрос
	return tcpConnectionItem.sendData(requestData)
}

// -------------------------------------------------------
// PROTECTED
// -------------------------------------------------------

// получаем tcpPort по переданному слайсу
func getTcpPort(tcpPortSlice []int64) int64 {

	// присваиваем значение текущего модуля из конфига
	var tcpPort int64 = 0

	// присваваем элемент из слайса, если он там имеется
	if len(tcpPortSlice) > 0 {
		tcpPort = tcpPortSlice[0]
	}

	return tcpPort
}

// получаем соединение из пула
func (s *tcpConnectionStorage) getTcpConnectionItem(tcpPort int64) (*tcpConnectionStruct, error) {

	// блочим пул
	s.mu.Lock()

	// раблочиваем пул при выходе
	defer s.mu.Unlock()

	// забираем соединние из пула
	tcpConnectionItem, isSuccess := s.cache[tcpPort]
	if !isSuccess {

		// создаем новое соединение
		return s.createTcpConnectionItem(tcpPort)
	}

	return tcpConnectionItem, nil
}

// создаем соединение
func (s *tcpConnectionStorage) createTcpConnectionItem(tcpPort int64) (*tcpConnectionStruct, error) {

	// устанавливаем соединение
	tcpAddrItem, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", tcpPort))
	connectionItem, err := net.DialTCP("tcp", nil, tcpAddrItem)
	if err != nil {
		return &tcpConnectionStruct{}, err
	}

	// инициаилизируем объект для чтения ответа
	readerItem := bufio.NewReader(connectionItem)

	// создаем объект соединения
	tcpConnectionItem := &tcpConnectionStruct{
		connectionItem: connectionItem,
		readerItem:     readerItem,
		tcpPort:        tcpPort,
		reconnectCount: 0,
	}

	// добавляем соедиенение в пул
	s.cache[tcpPort] = tcpConnectionItem

	return tcpConnectionItem, nil
}

// удаляем соединение
func (s *tcpConnectionStorage) deleteTcpConnectionItem(tcpPort int64) {

	// блочим пул
	s.mu.Lock()

	// удаляем соединение
	delete(s.cache, tcpPort)

	// раблочиваем пул при выходе
	s.mu.Unlock()
}

// обращаемся по адресу
func (s *tcpConnectionStruct) sendData(requestData []byte) ([]byte, error) {

	// форматируем данные под memcache text protocol
	requestData = []byte(fmt.Sprintf("get %s\r\n", string(requestData)))

	// отправляем запрос
	_, err := s.connectionItem.Write(requestData)
	if err != nil {
		return s.resendData(requestData, err)
	}

	// получаем ответ
	responseData, err := s.getResponse()
	if err != nil {
		return s.resendData(requestData, err)
	}

	return responseData, nil
}

// в случае проваленого запроса - пытаемся переотправить с нового соединения
func (s *tcpConnectionStruct) resendData(requestData []byte, err error) ([]byte, error) {

	// проверяем кол-во переподключений
	if s.reconnectCount > maxReconnectCount {

		// закрываем соединение
		_ = s.close()

		return []byte{}, fmt.Errorf("max number of attempts reached\n\t- %v", err)
	}

	// пересоздаем соединение
	err = s.reconnect()
	if err != nil {

		// закрываем соединение
		_ = s.close()

		return []byte{}, fmt.Errorf("can't reconnect\n\t- %v", err)
	}

	// повторно отправляем запрос
	return s.sendData(requestData)
}

// разбираем ответ по TCP
func (s *tcpConnectionStruct) getResponse() ([]byte, error) {

	// иницализируем переменную для вывода
	var responseData []byte
	responseData = make([]byte, 32768)

	// считываем результат в переменную
	_, err := s.readerItem.Read(responseData)
	if err != nil {
		return nil, err
	}

	// форматируем данные из под memcache text protocol
	responseSlice := bytes.Split(responseData, []byte("\r\n"))

	// записываем сформатированный результат в переменную вывода
	if len(responseSlice) < 2 {
		return nil, fmt.Errorf("incorrect request from microservice")
	}
	responseData = responseSlice[1]

	return responseData, nil
}

// пересоздаем соединение
func (s *tcpConnectionStruct) reconnect() error {

	// устанавливаем соединение
	tcpAddrItem, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", s.tcpPort))
	connectionItem, err := net.DialTCP("tcp", nil, tcpAddrItem)
	if err != nil {
		return err
	}

	// инициаилизируем объект для чтения ответа
	readerItem := bufio.NewReader(connectionItem)

	// обновляем данные соединения
	s.connectionItem = connectionItem
	s.readerItem = readerItem
	s.reconnectCount = s.reconnectCount + 1

	return nil
}

// закрываем соедиение
func (s *tcpConnectionStruct) close() error {

	// удаляем соединение из пула
	tcpConnectionStore.deleteTcpConnectionItem(s.tcpPort)

	// закрываем фактичесткое соединение
	return s.connectionItem.Close()
}
