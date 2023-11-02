package flags

import (
	"flag"
	"github.com/getCompassUtils/go_base_frame/api/system/log"
	"os"
	"path/filepath"
)

// -------------------------------------------------------
// пакет, считывающий флаги командной строки,
// переданные в виде -name=value
// например ./talking.py update -logsdir=/home/go_talking_handler/
// -------------------------------------------------------

// переменные в которые записыаются значения командной строки
var (
	ConfDir       string // (confdir) путь к каталогу с файлами конфигурации
	ExecutableDir string // (executabledir) путь к исполняему файлу (необходимо для 'go test')
	LogsDir       string // (logsDir) путь к директории для хранения логов (если не указан - по дефолту папка logs  внутри проекта)
)

// парсим флаги
func Parse() {

	// пасим все флаги
	flag.Parse()
	log.UpdateLogDir(LogsDir)
}

// указываем флаги командной строки
func SetFlags() {

	var (
		defaultLogPath   string // директория для логов по умолчанию
		logPathImplement string
	)

	// устанавливаем значени переданного флага -confdir
	flag.StringVar(&ConfDir, "confdir", "", "path to the directory with configuration files")

	// устанавливаем значени переданного флага -executabledir
	flag.StringVar(&ExecutableDir, "executabledir", "", "path to the directory with executable file (needed to 'go test')")

	logPathImplement = "/logs/"

	if ExecutableDir == "" {

		// если дефолтная директория не передана получаем ее
		executablePath, _ := os.Executable()
		ExecutableDir = filepath.Dir(executablePath)

		// формируем путь до логов если директория не передана флагом
		logPathImplement = "/" + logPathImplement
	}

	defaultLogPath = ExecutableDir + logPathImplement

	// устанавливаем значени переданного флага -logsdir
	flag.StringVar(&LogsDir, "logsdir", defaultLogPath, "path to the directory with logs")
}
