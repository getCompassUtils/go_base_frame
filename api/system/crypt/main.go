package crypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/getCompassUtils/go_base_frame/api/system/log"
)

var cipherMethodKeyLength = map[string]int{
	"aes-128": 16,
	"aes-192": 24,
	"aes-256": 32,
}

const binarySha256Length = 32

// зашифровать строку
func Encrypt(src string, cipherMethod string, key string, iv string) (string, error) {

	// проверяем, что выбранный способ шифрования поддерживается
	if _, ok := cipherMethodKeyLength[cipherMethod]; !ok {
		return "", fmt.Errorf("not allowed cipher method")
	}

	// устанавливаем длину ключа
	keyLength := cipherMethodKeyLength[cipherMethod]

	keyByte := []byte(key)

	// проверяем, что переданный ключ подходит
	// если он меньше - дополняем нулями
	if len(keyByte) < keyLength {

		nul := make([]byte, keyLength-len(key))
		for i := 0; i < keyLength-len(key); i++ {
			nul[i] = 0
		}
		keyByte = append(keyByte, nul...)
	}

	// если он больше - обрезаем
	if len(key) > keyLength {
		keyByte = keyByte[:keyLength]
	}

	//
	block, err := aes.NewCipher(keyByte)
	if err != nil {
		fmt.Println("key error1", err)
	}
	if src == "" {
		fmt.Println("plain content empty")
	}

	// отрезаем от вектора инициализации 16 символов
	ivByte := ([]byte(iv))[:16]

	// шифруем контент
	ecb := cipher.NewCBCEncrypter(block, ivByte)
	content := []byte(src)
	content = PKCS5Padding(content, block.BlockSize())
	encrypted := make([]byte, len(content))
	ecb.CryptBlocks(encrypted, content)

	// создаем подпись
	h := hmac.New(sha256.New, []byte(key))
	h.Write(encrypted)

	// складываем подпись и зашифрованный контент6 переводим в base64
	encryptedString := base64.StdEncoding.EncodeToString([]byte(string(h.Sum(nil)) + string(encrypted)))
	return encryptedString, nil
}

// расшифровать строку
func Decrypt(encryptedString string, cipherMethod string, key string, iv string) (string, error) {

	// проверяем, что выбранный способ шифрования поддерживается
	if _, ok := cipherMethodKeyLength[cipherMethod]; !ok {
		return "", fmt.Errorf("not allowed cipher method")
	}

	// устанавливаем длину ключа
	keyLength := cipherMethodKeyLength[cipherMethod]
	if len(key) != keyLength {

		log.Errorf("you should pass 32 bit key")
		return "", fmt.Errorf("you should pass 32 bit key")
	}

	// декодируем base64 строку
	encrypted, err := base64.StdEncoding.DecodeString(encryptedString)

	// отрезаем от зашифрованной строки именно зашифрованные данные, отсекаем подпись
	encryptedSrc := encrypted[binarySha256Length:]
	hash := encrypted[:binarySha256Length]

	// проверяем подпись зашифрованного контента, что мы точно получили то, что и планировали
	h := hmac.New(sha256.New, []byte(key))
	h.Write(encryptedSrc)

	res := bytes.Compare(h.Sum(nil), hash)
	if res != 0 {
		return "", fmt.Errorf("hash check is failed")
	}

	block, err := aes.NewCipher([]byte(key))
	ivByte := ([]byte(iv))[:16]
	if err != nil {
		fmt.Println("key error1", err)
	}
	if len(encrypted) == 0 {
		fmt.Println("plain content empty")
	}

	// расшифровываем контент
	ecb := cipher.NewCBCDecrypter(block, ivByte)
	decrypted := make([]byte, len(encryptedSrc))

	ecb.CryptBlocks(decrypted, encryptedSrc)
	return string(PKCS5Trimming(decrypted)), nil
}

// превращаем слайс байтов в слайса 8-байтовых блоков, требуемых для алгоритма шифрования
func PKCS5Padding(ciphertext []byte, blockSize int) []byte {

	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// конвертим слайс расшифрованных байтов в удобоваримый вид
func PKCS5Trimming(encrypt []byte) []byte {

	padding := encrypt[len(encrypt)-1]
	return encrypt[:len(encrypt)-int(padding)]
}
