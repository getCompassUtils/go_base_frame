package server

import (
	"fmt"
	"github.com/getCompassUtils/go_base_frame/api/system/functions"
)

const (
	devTag        = "dev"
	ciTag         = "ci"
	masterTag     = "master"
	stageTag      = "stage"
	productionTag = "production"

	onPremiseTag = "on-premise"
	saasTag      = "saas"

	localTag       = "local"
	integrationTag = "integration"
)

// тип окружения
var environmentGroupTagList = []string{
	devTag,
	ciTag,
	masterTag,
	stageTag,
	productionTag,
}

// тип продукта
var productGroupTagList = []string{
	onPremiseTag,
	saasTag,
}

// мапа с тегами серверов
var serverTagMap map[string]bool

// инициализировать теги сервера
func Init(tl []string) error {

	if len(tl) < 2 {
		return fmt.Errorf("server tag list is invalid")
	}

	if len(functions.ListIntersection(tl, environmentGroupTagList)) != 1 {
		return fmt.Errorf("incorrect environment group tag")
	}

	if len(functions.ListIntersection(tl, productGroupTagList)) != 1 {
		return fmt.Errorf("incorrect product group tag")
	}

	serverTagMap = make(map[string]bool)
	for _, value := range tl {
		serverTagMap[value] = true
	}

	return nil
}

// явлеяется ли сервером для разработки
func IsDev() bool {

	return hasTag(devTag)
}

// является ли CI сервером
func IsCi() bool {

	return hasTag(ciTag)
}

// является ли stage сервером
func IsStage() bool {

	return hasTag(stageTag)
}

// является ли onpremise сервером
func IsOnPremise() bool {

	return hasTag(onPremiseTag)
}

// являетя ли saas сервером
func IsSaas() bool {

	return hasTag(saasTag)
}

// является ли production сервером
func IsProduction() bool {

	return hasTag(productionTag)
}

// является ли локальным окружением
func IsLocal() bool {

	return hasTag(localTag)
}

// добавлена ли поддержка интеграций
func IsIntegration() bool {

	return hasTag(integrationTag)
}

// является ли мастер окружением
func IsMaster() bool {

	return hasTag(masterTag)
}

// является ли одним из тестовых серверов
func IsTest() bool {

	return IsDev() || IsCi() || IsMaster() || IsLocal()
}

// есть ли тег у сервера
func hasTag(tag string) bool {

	if _, ok := serverTagMap[tag]; ok {
		return true
	}

	return false
}
