package assert

import (
	"bytes"
	"fmt"
	"reflect"
)

// -------------------------------------------------------
// пакет проверки подтверждения соответсвия интерфейсов в тестах
// -------------------------------------------------------

// -------------------------------------------------------
// PUBLIC
// -------------------------------------------------------

// проверка равенства интерфейсов
func Equal(expectedInterface, actualInterface interface{}) error {

	// проверяем схожи ли объекты
	if !interfaceAreEqual(expectedInterface, actualInterface) {

		// форматируем интерфейсы
		expected, actual := formatInterfaces(expectedInterface, actualInterface)

		// ругаемся на несовпадение
		return fmt.Errorf("received parameters are not equal\n\t- expected:\t%s\n\t- actual:\t%s", expected, actual)
	}

	return nil
}

// -------------------------------------------------------
// PROTECTED
// -------------------------------------------------------

// определяем, схожи ли интерфейсы
func interfaceAreEqual(expectedInterface, actualInterface interface{}) bool {

	// проверяем nil интерфейсы
	if expectedInterface == nil || actualInterface == nil {
		return expectedInterface == actualInterface
	}

	// преобразуем ожидаемый интерфейс в массив байт
	expected, isSuccess := expectedInterface.([]byte)
	if !isSuccess {
		return reflect.DeepEqual(expectedInterface, actualInterface)
	}

	// преобразуем актульный интерфейс в массив байт
	actual, isSuccess := actualInterface.([]byte)
	if !isSuccess {
		return false
	}

	// проверяем nil результаты
	if expected == nil || actual == nil {
		return expected == nil && actual == nil
	}
	return bytes.Equal(expected, actual)
}

// форматируем интерфейсы
func formatInterfaces(expectedInterface, actualInterface interface{}) (string, string) {

	// если форматирвем разные типы
	if reflect.TypeOf(expectedInterface) != reflect.TypeOf(actualInterface) {
		return fmt.Sprintf("%T(%#v)", expectedInterface, expectedInterface), fmt.Sprintf("%T(%#v)", actualInterface, actualInterface)
	}

	// если формтируем слайс байт
	if reflect.TypeOf(expectedInterface) == reflect.TypeOf([]byte{}) {
		return string(expectedInterface.([]byte)), string(actualInterface.([]byte))
	}

	// если типы схожи
	return fmt.Sprintf("%#v", expectedInterface), fmt.Sprintf("%#v", actualInterface)
}
