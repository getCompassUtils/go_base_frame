// это пакет который является дополнительным модулем для всех остальных, и берет на себя
// большую часть функций которые могут использоваться многократно в различных модулях
package go_base_frame

import jsoniter "github.com/json-iterator/go"

var Json = jsoniter.ConfigCompatibleWithStandardLibrary
