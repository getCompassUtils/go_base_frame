package sanitizer

import (
	"fmt"
	"regexp"
	"strings"
)

const (

	// регулярка для поиска эмодзи
	EMOJI_REGEX = "[\\x{1F600}-\\x{1F64F}\\x{1F300}-\\x{1F5FF}\\x{1F680}-\\x{1F6FF}\\x{1F1E0}-\\x{1F1FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{1F900}-\\x{1F9FF}\\x{1FA70}-\\x{1FAFF}]|\\x{FE0F}|\\x{FE0E}|\\x{200D}"

	/**
	 * регулярка для поиска общезапрещенных спецсимволов из списка
	 *
	 * включает unicode-блоки:
	 * - управляющие символы: x{2000}-x{200F}
	 * - управляющие символы форматирования: x{202C}-x{202D}
	 * - невидимые управляющие символы: x{2060}-x{206F}
	 * - специальные символы: x{FFF0}-x{FFFF}
	 */
	COMMON_FORBIDDEN_CHARACTER_REGEX = "([\\x{2000}-\\x{200F}]|" +
		"[\\x{202C}-\\x{202D}]|" +
		"[\\x{2060}-\\x{206F}]|" +
		"[\\x{FFF0}-\\x{FFFF}])"

	// регулярка для поиска отдельного списка запрещенные спецсимволов
	SPECIAL_CHARACTER_REGEX = "[!\"#$%&()*+,./:;=?@[\\]_\x60{|}~<>]"

	// регулярка для поиска угловых скобок
	ANGLE_BRACKET_REGEX = "[<>]"

	/**
	 * регулярка для поиска fancy текста (соответствующие блоки юникода)
	 *
	 * включает unicode-блоки:
	 * - математические буквы и цифры: x{1D400}-x{1D7FF}
	 * - полуширинные и полноширинные формы: x{FF00}-x{FFEF}
	 * - модификаторы: x{02B0}-x{02FF}
	 * - буквоподобные символы: x{2100}-x{214F}
	 * - обрамлённые буквы и цифры: x{2460}-x{24FF}
	 * - box drawing и блоки: x{2500}-x{259F}
	 * - разные символы: x{2600}-x{26FF} (☀️☑️⚠️✈️)
	 * - декоративные символы: x{2700}-x{27BF}
	 * - нестандартные пробелы и знаки: x{0080}-x{00BF}
	 * - надстрочные и подстрочные символы: x{2070}-x{209F}
	 * - символы: × © ® ¶ ∆ π • Þ ÷ þ
	 */
	FANCY_TEXT_REGEX = "[\\x{1D400}-\\x{1D7FF}]|[\\x{FF00}-\\x{FFEF}]|" +
		"[\\x{02B0}-\\x{02FF}]|" +
		"[\\x{2100}-\\x{214F}]|" +
		"[\\x{2460}-\\x{24FF}]|" +
		"[\\x{2500}-\\x{259F}]|" +
		"[\\x{2600}-\\x{26FF}]|" +
		"[\\x{2700}-\\x{27BF}]|" +
		"[\\x{0080}-\\x{00BF}]|" +
		"[\\x{2070}-\\x{209F}]|" +
		"\\x{00D7}|" +
		"\\x{00A9}|" +
		"\\x{00AE}|" +
		"\\x{00B6}|" +
		"\\x{2206}|" +
		"\\x{03C0}|" +
		"\\x{2022}|" +
		"\\x{00DE}|" +
		"\\x{00F7}|" +
		"\\x{00FE}|"

	// регулярка для поиска двойного пробела
	DOUBLE_SPACE_REGEX = "[ ]{2,}"

	// регулярка для поиска любого переноса строки
	NEWLINE_REGEX = "\\n|\\r\\n"
)

// SanitizeString очистить строку от запрещенных символов
func SanitizeString(str string, regexps []string, replaces []string) (string, error) {

	if len(regexps) != len(replaces) {
		return "", fmt.Errorf("regexps length must be equal replaces length")
	}

	strByte := []byte(str)
	for i, r := range regexps {

		regex, err := regexp.Compile(r)

		if err != nil {
			return "", err
		}

		repl := []byte(replaces[i])

		strByte = regex.ReplaceAll(strByte, repl)
	}

	return strings.TrimSpace(string(strByte)), nil
}
