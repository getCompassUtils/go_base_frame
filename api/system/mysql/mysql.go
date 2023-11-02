package mysql

import (
	"database/sql"
	"fmt"
	"github.com/getCompassUtils/go_base_frame/api/system/functions"
	"github.com/getCompassUtils/go_base_frame/api/system/log"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"sync"
	"time"
)

// -------------------------------------------------------
// пакет-интерфейс для работы с базой данных
// создания подключений, осуществления запросов к таблицам
// -------------------------------------------------------

// ConnectionPoolItem структура объекта подключения к базе данных
type ConnectionPoolItem struct {
	Connection *sql.DB
	createdAt  int64
}

// объявляем хранилище
var mysqlConnectionPool = sync.Map{}

// TransactionStruct структура транзакции
type TransactionStruct struct {
	transaction *sql.Tx
}

// структура для форматирования ответа
type queryStruct struct {
	rows       *sql.Rows
	columnList []string
	valueList  []sql.RawBytes
	scanList   []interface{}
	err        error
}

// внутренний тип - счетчик ошибок
type countError struct {
	Count    int
	MaxCount int
	TimeWait int
}

// -------------------------------------------------------
// mysql
// -------------------------------------------------------

// ReplaceConnection обновить объект подключения
func ReplaceConnection(db string, conn *sql.DB) {

	// заносим подключение в кэш
	connectionPoolItem := ConnectionPoolItem{
		Connection: conn,
		createdAt:  functions.GetCurrentTimeStamp(),
	}

	// перезаписываем объект подключения
	connectionPoolItem.Connection = conn
	mysqlConnectionPool.Store(db, &connectionPoolItem)
}

// GetMysqlConnection получаем конфигурацию main
func GetMysqlConnection(db string, host string, user string, pass string, maxConnections int, isSsl bool, isNeedReconnect bool) *ConnectionPoolItem {

	uniqueKey := host + "-" + db
	connectionPoolItem, exist := mysqlConnectionPool.Load(uniqueKey)

	// если не было коннекта - создаем
	if exist && !isNeedReconnect {
		return connectionPoolItem.(*ConnectionPoolItem)
	}

	count := 0

	// пойдем в цикле обращаться, чтобы в случае ошибки можно было переотправить запрос
	for count <= 3 {

		mysqlConnection, err := openMysqlConnection(db, host, user, pass, maxConnections, isSsl)
		if err != nil {

			// увеличим счетчик ошибок на один
			count++

			log.Errorf("error when connect to database `%s`, err: %s", db, err.Error())

			// подождем 20 миллисекунд
			time.Sleep(time.Millisecond * 20)
			continue
		}

		log.Infof("Открыл соединение к базе %s", db)
		connectionPoolItem, _ = mysqlConnectionPool.LoadOrStore(uniqueKey, mysqlConnection)
		return connectionPoolItem.(*ConnectionPoolItem)
	}
	return nil
}

// открываем соединение
func openMysqlConnection(db string, host string, user string, pass string, maxConnections int, isSsl bool) (*ConnectionPoolItem, error) {

	connection, err := connectToDb(db, host, user, pass, maxConnections, isSsl)

	// заносим подключение в кэш
	connectionItem := ConnectionPoolItem{
		Connection: connection,
		createdAt:  functions.GetCurrentTimeStamp(),
	}

	return &connectionItem, err
}

// подключаемся в базе
func connectToDb(db string, host string, user string, pass string, maxConnections int, isSsl bool) (*sql.DB, error) {

	// получаем параметры подключения к базе данных
	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, host, db)

	// добавляем ssl, если нужно
	if isSsl {
		connectionString = fmt.Sprintf("%s%s", connectionString, "?tls=skip-verify")
	}

	connection, err := sql.Open("mysql", connectionString)
	if err != nil {

		log.Errorf("unable open mysql connection, database: '%s', error: %v", db, err)
		return connection, err
	}

	// ограничиваем кол-во одновременно открытых соединений с базой данных
	connection.SetMaxIdleConns(maxConnections)
	connection.SetMaxOpenConns(maxConnections)
	connection.SetConnMaxLifetime(time.Minute * 1)
	return connection, nil
}

// Ping функция для пинга соединения
func (connectionItem *ConnectionPoolItem) Ping() error {

	return connectionItem.Connection.Ping()
}

// -------------------------------------------------------
// QUERY
// -------------------------------------------------------

// GetAll получаем массив из запроса
func (connectionItem *ConnectionPoolItem) GetAll(query string, args ...interface{}) (map[int]map[string]string, error) {

	// осуществляем запрос
	queryItem, err := connectionItem.sendQueryForFormat(query, args...)
	if err != nil {

		log.Errorf("unable send query, error: %v", err)
		return map[int]map[string]string{}, err
	}

	response := queryItem.formatFetchArrayQuery()
	return response, queryItem.err
}

// FetchQuery получаем ответ после запроса
func (connectionItem *ConnectionPoolItem) FetchQuery(query string, args ...interface{}) (map[string]string, error) {

	// осуществляем запрос
	queryItem, err := connectionItem.sendQueryForFormat(query, args...)
	if err != nil {

		log.Errorf("unable send query, error: %v", err)
		return map[string]string{}, err
	}

	response := queryItem.formatFetchQuery()
	return response, queryItem.err
}

// FetchList получаем одномерный массив из запроса
func (connectionItem *ConnectionPoolItem) FetchList(query string, args ...interface{}) ([]string, error) {

	// осуществляем запрос
	queryItem, err := connectionItem.sendQueryForFormat(query, args...)
	if err != nil {

		log.Errorf("unable send query, error: %v", err)
		return []string{}, err
	}

	response := queryItem.formatFetchList()
	return response, queryItem.err
}

// Close закрываем соединение
func (connectionItem *ConnectionPoolItem) Close() error {

	return connectionItem.Connection.Close()
}

// Insert осуществляем запрос вставки
func (connectionItem *ConnectionPoolItem) Insert(tableName string, insert map[string]interface{}, isIgnore bool) (int64, error) {

	var keys, valueKeys string
	var values []interface{}
	for k, v := range insert {

		keys += fmt.Sprintf("`%s` , ", k)
		valueKeys += "? , "
		values = append(values, v)
	}

	keys = strings.TrimSuffix(keys, " , ")
	valueKeys = strings.TrimSuffix(valueKeys, " , ")

	ignore := ""
	if isIgnore {
		ignore = "IGNORE "
	}
	query := fmt.Sprintf("INSERT %sINTO %s (%s) VALUES (%s)", ignore, tableName, keys, valueKeys)
	res, err := connectionItem.Connection.Exec(query, values...)
	if err != nil {
		return 0, fmt.Errorf("query: %s, error: %v", query, err)
	}
	rows, _ := res.RowsAffected()
	return rows, nil
}

// InsertOrUpdate осуществляем запрос insert or update
func (connectionItem *ConnectionPoolItem) InsertOrUpdate(tableName string, insert map[string]interface{}) error {

	var keys, valueKeys, updateKeys string
	var values []interface{}
	for k, v := range insert {

		keys += fmt.Sprintf("`%s` , ", k)
		valueKeys += "? , "
		updateKeys += fmt.Sprintf("`%s` = ? , ", k)
		values = append(values, v)
	}

	values = append(values, values...)
	keys = strings.TrimSuffix(keys, " , ")
	valueKeys = strings.TrimSuffix(valueKeys, " , ")
	updateKeys = strings.TrimSuffix(updateKeys, " , ")

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) on duplicate key update %s;",
		tableName, keys, valueKeys, updateKeys)
	_, err := connectionItem.Connection.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("query: %s, error: %v", query, err)
	}

	return nil
}

// Update осуществляем запрос update
func (connectionItem *ConnectionPoolItem) Update(query string, args ...interface{}) (int64, error) {

	// проверяем соединение и осуществляем запрос
	res, err := connectionItem.Connection.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("query: %s, error: %v", query, err)
	}

	rows, _ := res.RowsAffected()
	return rows, nil
}

// Query осуществляем запрос
func (connectionItem *ConnectionPoolItem) Query(query string, args ...interface{}) error {

	// проверяем соединение и осуществляем запрос
	_, err := connectionItem.Connection.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("query: %s, error: %v", query, err)
	}

	return nil
}

// InsertArray функция для вставки массива записей в базу
func (connectionItem *ConnectionPoolItem) InsertArray(tableName string, columnList []string, insertDataList [][]interface{}) error {

	var columnString, valueString string
	for _, column := range columnList {
		columnString += column + ", "
	}

	columnString = columnString[:len(columnString)-2]

	var valueList []interface{}
	for _, insertData := range insertDataList {

		valueString += "("
		for _, v := range insertData {

			valueString += "?, "
			valueList = append(valueList, v)
		}
		valueString = valueString[:len(valueString)-2] + "), "
	}
	valueString = valueString[:len(valueString)-2]

	query := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES %s", tableName, columnString, valueString)
	return connectionItem.Query(query, valueList...)
}

// осуществляем запрос, возвращаем объект для форматирования ответа
func (connectionItem *ConnectionPoolItem) sendQueryForFormat(query string, args ...interface{}) (*queryStruct, error) {

	// инициализируем объект для обработки ответа
	queryItem := &queryStruct{}

	// осуществляем запрос
	queryItem.rows, queryItem.err = connectionItem.Connection.Query(query, args...)
	if queryItem.err != nil {
		return &queryStruct{}, fmt.Errorf("unable send query: '%s', error: %v", query, queryItem.err)
	}

	// получаем поля таблицы
	queryItem.columnList, queryItem.err = queryItem.rows.Columns()
	if queryItem.err != nil {

		_ = queryItem.rows.Close()
		return &queryStruct{}, fmt.Errorf("no table field in response, query: '%s', error: %v", query, queryItem.err)
	}

	// создаем массив значений и массив для rows.Scan
	queryItem.valueList = make([]sql.RawBytes, len(queryItem.columnList))
	queryItem.scanList = make([]interface{}, len(queryItem.valueList))
	for i := range queryItem.valueList {
		queryItem.scanList[i] = &queryItem.valueList[i]
	}

	return queryItem, nil
}

// -------------------------------------------------------
// TRANSACTION METHODS
// -------------------------------------------------------

// BeginTransaction начинаем транзакцию
func (connectionItem *ConnectionPoolItem) BeginTransaction() (TransactionStruct, error) {

	// начинаем транзакцию
	transactionItem, err := connectionItem.Connection.Begin()
	if err != nil {
		return TransactionStruct{nil}, err
	}
	return TransactionStruct{transactionItem}, nil
}

// InsertArray функция для вставки массива записей в базу
func (transactionItem *TransactionStruct) InsertArray(tableName string, columnList []string, insertDataList [][]interface{}, isIgnore bool) {

	var columnString = ""
	var valuesString = ""
	for _, column := range columnList {
		columnString += column + ", "
		valuesString += "?, "
	}
	if len(columnString) < 1 {
		return
	}

	columnString = columnString[:len(columnString)-2]
	valuesString = valuesString[:len(valuesString)-2]

	ignore := ""
	if isIgnore {
		ignore = "IGNORE "
	}
	query := fmt.Sprintf("INSERT %sINTO %s (%s) VALUES (%s)", ignore, tableName, columnString, valuesString)
	stmt, _ := transactionItem.transaction.Prepare(query)
	for _, v := range insertDataList {
		_, _ = stmt.Exec(v...)
	}
}

// FetchQuery получаем ответ после запроса
func (transactionItem *TransactionStruct) FetchQuery(query string, args ...interface{}) (map[string]string, error) {

	// осуществляем запрос
	queryItem, err := transactionItem.sendQueryForFormat(query, args...)
	if err != nil {

		log.Errorf("unable send query with transaction, error: %v", err)
		return map[string]string{}, err
	}

	response := queryItem.formatFetchQuery()
	return response, queryItem.err
}

// GetAll получаем массив
func (transactionItem *TransactionStruct) GetAll(query string, args ...interface{}) (map[int]map[string]string, error) {

	// осуществляем запрос
	queryItem, err := transactionItem.sendQueryForFormat(query, args...)
	if err != nil {

		log.Errorf("unable send query with transaction, error: %v", err)
		return map[int]map[string]string{}, err
	}

	response := queryItem.formatFetchArrayQuery()

	return response, queryItem.err
}

// Update осуществляем запрос update
func (transactionItem *TransactionStruct) Update(query string, args ...interface{}) (int64, error) {

	// проверяем соединение и осуществляем запрос
	res, err := transactionItem.transaction.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("query: %s, error: %v", query, err)
	}

	return res.RowsAffected()
}

// Commit подтверждаем транзакцию
func (transactionItem *TransactionStruct) Commit() error {

	// подтверждаем транзакцию
	err := transactionItem.transaction.Commit()
	if err != nil {

		log.Errorf("unable commit transaction, error: %v", err)
		return err
	}

	return nil
}

// Rollback откатываем транзакцию
func (transactionItem *TransactionStruct) Rollback() error {

	// откатываем транзакцию
	err := transactionItem.transaction.Rollback()
	if err != nil {

		log.Errorf("unable rollback transaction, error: %v", err)
		return err
	}

	return nil
}

// ExecQuery осуществляем запрос
func (transactionItem *TransactionStruct) ExecQuery(query string, args ...interface{}) error {

	// осуществляем запрос
	_, err := transactionItem.transaction.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("transaction query: %s, error: %v", query, err)
	}

	return nil
}

// осуществляем запрос, возвращаем объект для форматирования ответа
func (transactionItem *TransactionStruct) sendQueryForFormat(query string, args ...interface{}) (*queryStruct, error) {

	// инициализируем объект для обработки ответа
	queryItem := &queryStruct{}

	// осуществляем запрос
	queryItem.rows, queryItem.err = transactionItem.transaction.Query(query, args...)
	if queryItem.err != nil {

		_ = queryItem.rows.Close()
		return &queryStruct{}, fmt.Errorf("unable send query: '%s', error: %v", query, queryItem.err)
	}

	// получаем поля таблицы
	queryItem.columnList, queryItem.err = queryItem.rows.Columns()
	if queryItem.err != nil {

		_ = queryItem.rows.Close()
		return &queryStruct{}, fmt.Errorf("no table field in response, query: '%s', error: %v", query, queryItem.err)
	}

	// создаем массив значений и массив для rows.Scan
	queryItem.valueList = make([]sql.RawBytes, len(queryItem.columnList))
	queryItem.scanList = make([]interface{}, len(queryItem.valueList))
	for i := range queryItem.valueList {
		queryItem.scanList[i] = &queryItem.valueList[i]
	}

	return queryItem, nil
}

// InsertOrUpdate осуществляем запрос insert or update
func (transactionItem *TransactionStruct) InsertOrUpdate(tableName string, insert map[string]interface{}) error {

	var keys, valueKeys, updateKeys string
	var values []interface{}
	for k, v := range insert {

		keys += fmt.Sprintf("`%s` , ", k)
		valueKeys += "? , "
		updateKeys += fmt.Sprintf("`%s` = ? , ", k)
		values = append(values, v)
	}

	values = append(values, values...)
	keys = strings.TrimSuffix(keys, " , ")
	valueKeys = strings.TrimSuffix(valueKeys, " , ")
	updateKeys = strings.TrimSuffix(updateKeys, " , ")

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) on duplicate key update %s;",
		tableName, keys, valueKeys, updateKeys)

	err := transactionItem.ExecQuery(query, values...)
	if err != nil {
		return fmt.Errorf("query: %s, error: %v", query, err)
	}
	return nil
}

// -------------------------------------------------------
// FORMATTING METHODS
// -------------------------------------------------------

// форматируем запрос в массив
func (queryItem *queryStruct) formatFetchArrayQuery() map[int]map[string]string {

	defer queryItem.afterFetchQuery()

	// создаем массив ответа и заполняем его
	resultMap := make(map[int]map[string]string)
	for i := 0; queryItem.rows.Next(); i++ {

		// сканируем строку и обрабатываем ошибку
		queryItem.handleError(queryItem.rows.Scan(queryItem.scanList...))
		if queryItem.err != nil {
			return nil
		}

		// заполняем массив
		rowArray := make(map[string]string)
		for key, value := range queryItem.valueList {
			rowArray[queryItem.columnList[key]] = string(value)
		}
		resultMap[i] = rowArray
	}

	return resultMap
}

// форматируем запрос
func (queryItem *queryStruct) formatFetchQuery() map[string]string {

	defer queryItem.afterFetchQuery()

	// создаем массив ответа и заполняем его
	resultMap := make(map[string]string)
	for queryItem.rows.Next() {

		// сканируем строку и обрабатываем ошибку
		queryItem.handleError(queryItem.rows.Scan(queryItem.scanList...))
		if queryItem.err != nil {
			return nil
		}

		// заполняем массив
		for key, value := range queryItem.valueList {
			resultMap[queryItem.columnList[key]] = string(value)
		}
	}

	return resultMap
}

// форматируем запрос в одномерный массив
func (queryItem *queryStruct) formatFetchList() []string {

	defer queryItem.afterFetchQuery()

	// создаем массив ответа и заполняем его
	var resultMap []string
	for queryItem.rows.Next() {

		// сканируем строку и обрабатываем ошибку
		queryItem.handleError(queryItem.rows.Scan(queryItem.scanList...))
		if queryItem.err != nil {
			return nil
		}

		// заполняем массив
		for _, value := range queryItem.valueList {
			resultMap = append(resultMap, string(value))
		}
	}

	return resultMap
}

// после fetch query
func (queryItem *queryStruct) afterFetchQuery() {

	queryItem.handleError(queryItem.rows.Err())
	queryItem.handleError(queryItem.rows.Close())
}

// обрабатываем ошибку
func (queryItem *queryStruct) handleError(err error) {

	if err != nil {
		queryItem.err = err
	}
}
