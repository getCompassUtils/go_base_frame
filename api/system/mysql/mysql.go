package mysql

import (
	"context"
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

// таймауты нужны, чтобы зависший запрос не занял надолго подключение к базе.
// подключений ограниченное количество, а значит есть риск создать очередь запросов из-за зависших подключений
const pingTimeout = 200 * time.Millisecond // таймаут для пинга
const QueryTimeout = 5 * time.Second       // таймаут для запросов

// ConnectionPoolItem структура объекта подключения к базе данных
type ConnectionPoolItem struct {
	ConnectionPool *sql.DB
	createdAt      int64
}

// объявляем хранилище
var mysqlConnectionPoolList = sync.Map{}

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

// -------------------------------------------------------
// mysql
// -------------------------------------------------------

// ReplaceConnection обновить объект пула подключений
func ReplaceConnection(db string, conn *sql.DB) {

	// заносим подключение в кэш
	connectionPoolItem := ConnectionPoolItem{
		ConnectionPool: conn,
		createdAt:      functions.GetCurrentTimeStamp(),
	}

	// перезаписываем объект подключения
	connectionPoolItem.ConnectionPool = conn
	mysqlConnectionPoolList.Store(db, &connectionPoolItem)
}

// CreateMysqlConnection создаем mysql подключение без сохранения в мапу
func CreateMysqlConnection(ctx context.Context, db string, host string, user string, pass string, maxConnections int, isSsl bool) (*ConnectionPoolItem, error) {

	// cоздаем пул соединений с mysql
	mysqlConnectionPool, err := openMysqlConnectionPool(db, host, user, pass, maxConnections, isSsl)

	// !!! СОЗДАНИЕ ПУЛА НЕ ПРОВЕРЯЕТ НАЛИЧИЕ СОЕДИНЕНИЯ
	if err != nil {

		log.Errorf("error when creating db connection pool `%s`, err: %s", db, err.Error())
		return nil, err
	}

	// устанавливаем первое соединение, сразу проверяя, что база доступна
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	err = mysqlConnectionPool.ConnectionPool.PingContext(ctx)

	if err != nil {

		log.Errorf("error when connect to database `%s`, err: %s", db, err.Error())
		return nil, err
	}

	log.Infof("Открыл соединение к базе %s", db)
	return mysqlConnectionPool, nil
}

// GetMysqlConnection получаем хранимое mysql подключение
func GetMysqlConnection(ctx context.Context, db string, host string, user string, pass string, maxConnections int, isSsl bool) (*ConnectionPoolItem, error) {

	uniqueKey := host + "-" + db
	mysqlConnectionPool, exist := mysqlConnectionPoolList.Load(uniqueKey)

	// если не было пула соединений - создаем
	if exist {
		return mysqlConnectionPool.(*ConnectionPoolItem), nil
	}

	mysqlConnectionPool, err := openMysqlConnectionPool(db, host, user, pass, maxConnections, isSsl)

	// !!! СОЗДАНИЕ ПУЛА НЕ ПРОВЕРЯЕТ НАЛИЧИЕ СОЕДИНЕНИЯ
	if err != nil {

		log.Errorf("error when creating db connection pool `%s`, err: %s", db, err.Error())
		return nil, err
	}

	// устанавливаем первое соединение, сразу проверяя, что база доступна
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	err = mysqlConnectionPool.(*ConnectionPoolItem).ConnectionPool.PingContext(ctx)

	if err != nil {

		log.Errorf("error when connect to database `%s`, err: %s", db, err.Error())
		return nil, err
	}

	log.Infof("Открыл соединение к базе %s", db)
	mysqlConnectionPoolList.Store(uniqueKey, mysqlConnectionPool)

	return mysqlConnectionPool.(*ConnectionPoolItem), nil
}

// открываем соединение
func openMysqlConnectionPool(db string, host string, user string, pass string, maxConnections int, isSsl bool) (*ConnectionPoolItem, error) {

	connectionPool, err := connectToDb(db, host, user, pass, maxConnections, isSsl)

	// заносим подключение в кэш
	connectionPoolItem := ConnectionPoolItem{
		ConnectionPool: connectionPool,
		createdAt:      functions.GetCurrentTimeStamp(),
	}

	return &connectionPoolItem, err
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
	connection.SetMaxOpenConns(maxConnections)
	connection.SetMaxIdleConns(maxConnections)
	connection.SetConnMaxLifetime(time.Minute * 1)
	return connection, nil
}

// RemoveMysqlConnectionPool удаляем пул соединений для базы и хоста
func RemoveMysqlConnectionPool(db string, host string) error {

	uniqueKey := host + "-" + db

	item, exist := mysqlConnectionPoolList.Load(uniqueKey)

	// если не было пула соединений - то и закрывать нечего
	if !exist {
		return nil
	}
	mysqlConnectionPool := item.(*ConnectionPoolItem)

	// закрываем соединение
	err := mysqlConnectionPool.Close()

	if err != nil {
		return fmt.Errorf("cant close db on host %s for db %s", host, db)
	}

	mysqlConnectionPoolList.Delete(uniqueKey)
	return nil
}

// Ping функция для пинга соединения
func (connectionItem *ConnectionPoolItem) Ping() error {

	return connectionItem.ConnectionPool.Ping()
}

// -------------------------------------------------------
// QUERY
// -------------------------------------------------------

// GetAll получаем массив из запроса
func (connectionItem *ConnectionPoolItem) GetAll(ctx context.Context, query string, args ...interface{}) (map[int]map[string]string, error) {

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	// осуществляем запрос
	queryItem, err := connectionItem.sendQueryForFormat(queryContext, query, args...)
	if err != nil {

		log.Errorf("unable send query, error: %v", err)
		return map[int]map[string]string{}, err
	}

	response := queryItem.formatFetchArrayQuery()
	return response, queryItem.err
}

// FetchQuery получаем ответ после запроса
func (connectionItem *ConnectionPoolItem) FetchQuery(ctx context.Context, query string, args ...interface{}) (map[string]string, error) {

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	// осуществляем запрос
	queryItem, err := connectionItem.sendQueryForFormat(queryContext, query, args...)
	if err != nil {

		log.Errorf("unable send query, error: %v", err)
		return map[string]string{}, err
	}

	response := queryItem.formatFetchQuery()
	return response, queryItem.err
}

// FetchList получаем одномерный массив из запроса
func (connectionItem *ConnectionPoolItem) FetchList(ctx context.Context, query string, args ...interface{}) ([]string, error) {

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	// осуществляем запрос
	queryItem, err := connectionItem.sendQueryForFormat(queryContext, query, args...)
	if err != nil {

		log.Errorf("unable send query, error: %v", err)
		return []string{}, err
	}

	response := queryItem.formatFetchList()
	return response, queryItem.err
}

// Close закрываем соединение
func (connectionItem *ConnectionPoolItem) Close() error {

	return connectionItem.ConnectionPool.Close()
}

// Insert осуществляем запрос вставки
func (connectionItem *ConnectionPoolItem) Insert(ctx context.Context, tableName string, insert map[string]interface{}, isIgnore bool) (int64, error) {

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

	queryCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	res, err := connectionItem.ConnectionPool.ExecContext(queryCtx, query, values...)
	if err != nil {
		return 0, fmt.Errorf("query: %s, error: %v", query, err)
	}
	rows, _ := res.RowsAffected()
	return rows, nil
}

// InsertOrUpdate осуществляем запрос insert or update
func (connectionItem *ConnectionPoolItem) InsertOrUpdate(ctx context.Context, tableName string, insert map[string]interface{}) error {

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

	queryCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	_, err := connectionItem.ConnectionPool.ExecContext(queryCtx, query, values...)
	if err != nil {
		return fmt.Errorf("query: %s, error: %v", query, err)
	}

	return nil
}

// Update осуществляем запрос update
func (connectionItem *ConnectionPoolItem) Update(ctx context.Context, query string, args ...interface{}) (int64, error) {

	queryCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	// проверяем соединение и осуществляем запрос
	res, err := connectionItem.ConnectionPool.ExecContext(queryCtx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("query: %s, error: %v", query, err)
	}

	rows, _ := res.RowsAffected()
	return rows, nil
}

// Query осуществляем запрос
func (connectionItem *ConnectionPoolItem) Query(ctx context.Context, query string, args ...interface{}) error {

	// проверяем соединение и осуществляем запрос
	_, err := connectionItem.ConnectionPool.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query: %s, error: %v", query, err)
	}

	return nil
}

// InsertArray функция для вставки массива записей в базу
func (connectionItem *ConnectionPoolItem) InsertArray(ctx context.Context, tableName string, columnList []string, insertDataList [][]interface{}) error {

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

	queryCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	return connectionItem.Query(queryCtx, query, valueList...)
}

// осуществляем запрос, возвращаем объект для форматирования ответа
func (connectionItem *ConnectionPoolItem) sendQueryForFormat(ctx context.Context, query string, args ...interface{}) (*queryStruct, error) {

	// инициализируем объект для обработки ответа
	queryItem := &queryStruct{}

	// осуществляем запрос
	queryItem.rows, queryItem.err = connectionItem.ConnectionPool.QueryContext(ctx, query, args...)
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
	transactionItem, err := connectionItem.ConnectionPool.Begin()
	if err != nil {
		return TransactionStruct{nil}, err
	}
	return TransactionStruct{transactionItem}, nil
}

// InsertArray функция для вставки массива записей в базу
func (transactionItem *TransactionStruct) InsertArray(ctx context.Context, tableName string, columnList []string, insertDataList [][]interface{}, isIgnore bool) {

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

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	query := fmt.Sprintf("INSERT %sINTO %s (%s) VALUES (%s)", ignore, tableName, columnString, valuesString)
	stmt, _ := transactionItem.transaction.Prepare(query)
	for _, v := range insertDataList {
		_, _ = stmt.ExecContext(queryContext, v...)
	}
}

// FetchQuery получаем ответ после запроса
func (transactionItem *TransactionStruct) FetchQuery(ctx context.Context, query string, args ...interface{}) (map[string]string, error) {

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	// осуществляем запрос
	queryItem, err := transactionItem.sendQueryForFormat(queryContext, query, args...)
	if err != nil {

		log.Errorf("unable send query with transaction, error: %v", err)
		return map[string]string{}, err
	}

	response := queryItem.formatFetchQuery()
	return response, queryItem.err
}

// GetAll получаем массив
func (transactionItem *TransactionStruct) GetAll(ctx context.Context, query string, args ...interface{}) (map[int]map[string]string, error) {

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	// осуществляем запрос
	queryItem, err := transactionItem.sendQueryForFormat(queryContext, query, args...)
	if err != nil {

		log.Errorf("unable send query with transaction, error: %v", err)
		return map[int]map[string]string{}, err
	}

	response := queryItem.formatFetchArrayQuery()

	return response, queryItem.err
}

// Update осуществляем запрос update
func (transactionItem *TransactionStruct) Update(ctx context.Context, query string, args ...interface{}) (int64, error) {

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	// проверяем соединение и осуществляем запрос
	res, err := transactionItem.transaction.ExecContext(queryContext, query, args...)
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
func (transactionItem *TransactionStruct) ExecQuery(ctx context.Context, query string, args ...interface{}) error {

	// осуществляем запрос
	_, err := transactionItem.transaction.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("transaction query: %s, error: %v", query, err)
	}

	return nil
}

// осуществляем запрос, возвращаем объект для форматирования ответа
func (transactionItem *TransactionStruct) sendQueryForFormat(ctx context.Context, query string, args ...interface{}) (*queryStruct, error) {

	// инициализируем объект для обработки ответа
	queryItem := &queryStruct{}

	// осуществляем запрос
	queryItem.rows, queryItem.err = transactionItem.transaction.QueryContext(ctx, query, args...)
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

// Insert осуществляем запрос insert
func (transactionItem *TransactionStruct) Insert(ctx context.Context, tableName string, insert map[string]interface{}, isIgnore bool) error {

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

	queryCtx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	err := transactionItem.ExecQuery(queryCtx, query, values...)
	if err != nil {
		return fmt.Errorf("query: %s, error: %v", query, err)
	}
	return nil
}

// InsertOrUpdate осуществляем запрос insert or update
func (transactionItem *TransactionStruct) InsertOrUpdate(ctx context.Context, tableName string, insert map[string]interface{}) error {

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

	queryContext, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) on duplicate key update %s;",
		tableName, keys, valueKeys, updateKeys)

	err := transactionItem.ExecQuery(queryContext, query, values...)
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

// готовим запрос для InsertOrUpdate
func FormatInsertOrUpdate(tableName string, insert map[string]interface{}) (string, []interface{}) {

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
	return query, values
}

// готовим запрос для InsertArray
func InsertArray(tableName string, columnList []string, insertDataList [][]interface{}) (string, []interface{}) {

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
	return query, valueList
}
