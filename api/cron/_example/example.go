package example

import (
	"github.com/getCompassUtils/go_base_frame/api/cron"
	"github.com/getCompassUtils/go_base_frame/api/system/log"
)

// -------------------------------------------------------
// пример крона
// при получении задачи проверяет соединения пользователя
// -------------------------------------------------------

// структура задачи для крона
type taskDataStruct struct {
	needWork  int64
	userId    int64
	threadKey string
}

// объявляем переменную крона
var cronCheckUserConnection *cron.Struct

// -------------------------------------------------------
// PUBLIC
// -------------------------------------------------------

// пример функции
func Main() {

	// запускаем крон
	StartCron()

	// добавляем тестовую задачу
	AddTestTask()
}

// запускаем крон
func StartCron() {

	// запускаем крон
	config := cron.Config{
		MaxQueueSize: 1000,   // максимальное количество задач, которое может быть в очереди
		WorkerCount:  10,     // количество воркеров
		WorkerFunc:   doWork, // воркер
	}
	cronCheckUserConnection = cron.Start(config)
}

// добавляем тестовую задачу
func AddTestTask() {

	// формируем задачу
	taskData := taskDataStruct{
		userId:    1,
		threadKey: "somethingKey",
	}

	// добавлем задачу крону
	err := cronCheckUserConnection.AddTask(taskData)
	if err != nil {
		log.Errorf("%v", err)
	}
}

// -------------------------------------------------------
// PROTECTED
// -------------------------------------------------------

// воркер
func doWork(task *cron.TaskStruct) {

	// оставляем, чтобы не было подсветки файла
	_ = task

	// -------------------------------------------------------
	// ниже закоментированный участок кода, как пример
	// -------------------------------------------------------

	// // преобразуем задачу
	// taskData := task.DataInterface.(taskDataStruct)
	//
	// // удаляем лишние соединения, если не пришло подтверждения открытия треда от клиента
	// typingThreadStore.cleanConnectionsIfNeeded(taskData.threadKey, taskData.userId)
}
