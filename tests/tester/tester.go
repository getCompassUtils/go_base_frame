package tester

import (
	"encoding/json"
	"fmt"
	"github.com/getCompassUtils/go_base_frame/api/system/flags"
	"github.com/getCompassUtils/go_base_frame/api/system/functions"
	"github.com/getCompassUtils/go_base_frame/api/system/log"
	"github.com/getCompassUtils/go_base_frame/tests/tester/assert"
	"github.com/getCompassUtils/go_base_frame/tests/tester/tcp"
	"os"
	"runtime"
)

// выполняется при инициализации пакета
func init() {

	flags.SetFlags()
}

// -------------------------------------------------------
// пакет для проведения тестов
// содержит набор базовых функций
// -------------------------------------------------------

// интерфейс, перенимающий функции стандартного фреймворка
type testInterface interface {
	Name() string                              // выводим имя теста
	Logf(format string, args ...interface{})   // выводим лог
	Skip(args ...interface{})                  // пропускаем тест
	Errorf(format string, args ...interface{}) // выводим ошибку
	FailNow()                                  // останавливаем выполнение
	Helper()                                   // указываем что вызванная функция, является вспомогательной
}

// структура, для проведения тестов
type IStruct struct {
	fileName      string        // путь до теста
	testInterface testInterface // тестовый интерфейс
	tcpDataItem   tcpDataStruct // данные переданные по TCP
}

// структура данных переданных по TCP
type tcpDataStruct struct {
	requestMethod string                 // вызываемый метод
	requestMap    map[string]interface{} // передаваемые данные
	responseMap   map[string]interface{} // полученные данные
}

// структура ошибки
type ErrorStruct struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// -------------------------------------------------------
// PUBLIC
// -------------------------------------------------------

// стартуем тест, просто возвращая структуру для тестов
func StartTest(testInterface testInterface) IStruct {

	flags.Parse()

	// формируем структуру для тестов
	var I IStruct
	I.testInterface = testInterface

	// сохраняем путь до файла с тестом
	_, I.fileName, _, _ = runtime.Caller(1)

	return I
}

// сообщаем что именно в тесте мы хотим проверить
func (I *IStruct) WantToTest(formatText string, formatArgs ...interface{}) {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// форматируем переданый текст
	wantToTestText := fmt.Sprintf(formatText, formatArgs...)

	// отображаем проверку в выводе
	I.testInterface.Logf("\030-\030 %s", wantToTestText)
}

// пропускаем тест
func (I *IStruct) WantToSkip(formatText string, formatArgs ...interface{}) {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// форматируем переданый текст
	wantToSkipText := fmt.Sprintf(formatText, formatArgs...)

	// отображаем пропуск в выводе
	I.testInterface.Skip("\030-\030", wantToSkipText)
}

// обращаемся к микросервису по TCP
func (I *IStruct) CallTcp(method string, requestMap map[string]interface{}, tcpPort ...int64) *IStruct {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// записываем запрос
	I.tcpDataItem.requestMethod, requestMap["method"] = method, method
	I.tcpDataItem.requestMap = requestMap

	// переводим requestMap в JSON
	requestData, _ := json.Marshal(requestMap)

	// обращаемся по TCP
	responseData, err := tcp.Call(requestData, tcpPort...)
	if err != nil {
		I.Fail("TCP request failed\n\t- %v", err)
	}

	// переводим ответ в map
	err = json.Unmarshal(responseData, &I.tcpDataItem.responseMap)
	if err != nil {
		I.Fail("TCP request failed\n\t- %v", err)
	}

	return I
}

// возращаем ответ полученный по TCP
func (I *IStruct) GetLastResponse() map[string]interface{} {

	// возрашаем ответ из структуры
	return I.tcpDataItem.responseMap
}

// проверка равенства интерфейсов
func (I *IStruct) AssertEqual(expectedInterface, actualInterface interface{}) {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// проверяем равенство
	err := assert.Equal(expectedInterface, actualInterface)
	if err != nil {

		// ругаемся в случае провала
		I.Fail(err.Error())
	}
}

// проверка на ошибку
func (I *IStruct) AssertError(response interface{}, errorCode int64) {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	request := ErrorStruct{}

	byteRequest, _ := json.Marshal(response)
	err := json.Unmarshal(byteRequest, &request)
	if err != nil {
		panic(err)
	}

	// проверяем что пришла ожидаемая ошибка
	err = assert.Equal(int64(request.ErrorCode), errorCode)
	if err != nil {

		// ругаемся в случае провала
		I.Fail(err.Error())
	}
}

// подготавливаем map, необходимо передавать указатель
func (I *IStruct) PrepareMap(prepareMap interface{}) *IStruct {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// переводим prepareMap в JSON
	prepareData, err := json.Marshal(prepareMap)
	if err != nil {
		I.Fail("prepare map failed\n\t- %v", err)
	}

	// переводим JSON в map
	err = json.Unmarshal(prepareData, prepareMap)
	if err != nil {
		I.Fail("prepare map failed\n\t- %v", err)
	}

	return I
}

// ругаемся на ошибку
func (I *IStruct) Fail(formatError string, formatArgs ...interface{}) {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// добавляем метод в логи проваленных
	saveToFailed(I.fileName, I.testInterface.Name())

	// форматируем переданый текст
	errorText := fmt.Sprintf(formatError, formatArgs...)

	// логируем что тест не выполнен
	log.Errorf("Test not complete. Test struct:\n%+v", I)

	// выбрасываем ошибку
	I.testInterface.Errorf("\nError:\t%s", errorText)
	I.testInterface.FailNow()
}

// ругаемся на ошибку
func (I *IStruct) Console(format string, formatArgs ...interface{}) {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// выводим
	I.testInterface.Logf(format, formatArgs...)
}

// проверяем структуру
func (I *IStruct) CheckJsonStruct(checkedStruct interface{}, checkedType interface{}) {

	I.testInterface.Helper() // указываем что данная функция, является вспомогательной

	// проверяем структуру
	fromJsonStruct, _ := json.Marshal(checkedStruct)

	err := json.Unmarshal(fromJsonStruct, checkedType)
	if err != nil {

		I.Fail(err.Error())
	}
}

// -------------------------------------------------------
// PROTECTED
// -------------------------------------------------------

// добавляем метод в логи проваленных
func saveToFailed(testFileName string, testName string) {

	// формируем лог
	failedTest := fmt.Sprintf("%s:%s\n", testFileName, testName)

	// получаем местонахождение тестов
	testsDir := functions.GetExecutableDir(flags.ExecutableDir) + "/tests/"

	// создаем _output
	_ = os.Mkdir(testsDir+"tests", os.ModePerm)

	// пишем проваленный тест в файл
	writeToFile(testsDir+"tests/failed", failedTest)
}

// пишем сообщение в файл
func writeToFile(fileName string, message string) {

	// создаем файл если не был создан
	err := functions.CreateFileIfNotExist(fileName)
	if err != nil {

		log.Errorf("unable create file '%s', error: %v", fileName, err)
		return
	}

	// открываем файл на чтение
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {

		log.Errorf("unable open file `%s`, error: %v", fileName, err)
		return
	}

	// пишем в файл
	_, _ = file.WriteString(message)

	// закрываем файл
	_ = file.Close()
}
