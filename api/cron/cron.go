package cron

import (
	"fmt"
)

// -------------------------------------------------------
// пакет для конроля кронов
// -------------------------------------------------------

// конфигурация крона
type Config struct {
	MaxQueueSize int    // максимальное количество задач, которое может быть в очереди
	WorkerCount  int    // количество воркеров
	WorkerFunc   Worker // воркер
}

// структура крона
type Struct struct {
	workerFunc Worker      // воркер
	queueChan  taskChannel // канал очереди
}

// структура задачи
type TaskStruct struct {
	DataInterface interface{} // интерфес содержащий любые данные
}

// функция воркера
type Worker func(*TaskStruct)

// тип канала с задачами
type taskChannel chan *TaskStruct

// -------------------------------------------------------
// PUBLIC
// -------------------------------------------------------

// запускаем крон
func Start(config Config) *Struct {

	// создаем крон
	cron := &Struct{
		workerFunc: config.WorkerFunc,                      // передаем воркер
		queueChan:  make(taskChannel, config.MaxQueueSize), // создаем канал очереди
	}

	// запускаем воркеры
	for i := 0; i < config.WorkerCount; i++ {
		go cron.listenQueueChan()
	}

	return cron
}

// добавляем задачу в крон
func (cron *Struct) AddTask(dataInterface interface{}) error {

	// создаем задачу
	task := &TaskStruct{
		DataInterface: dataInterface,
	}

	// добавляем задачу в канал очереди
	err := task.addToChannel(cron.queueChan)
	if err != nil {
		return fmt.Errorf("unable add task to queue, error: %v", err)
	}

	return nil
}

// -------------------------------------------------------
// PROTECTED
// -------------------------------------------------------

// добавляем задачу в канал
func (task *TaskStruct) addToChannel(channel taskChannel) error {

	select {
	case channel <- task:
	default:
		return fmt.Errorf("channel is full")
	}

	return nil
}

// слушаем канал для получения новых задач
func (cron *Struct) listenQueueChan() {

	// бесконечный цикл
	for {
		// получаем задачу из канала
		task := <-cron.queueChan

		// отдаем задачу в воркер
		cron.workerFunc(task)
	}
}
