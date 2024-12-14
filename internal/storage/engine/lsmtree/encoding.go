package lsmtree

import (
	"fmt"
	"io"

	"github.com/bytedance/sonic"
)

// encode 函数对键和值进行编码，并将其写入指定的写入器。
// 返回写入的字节数以及可能发生的错误。
// 该函数必须与 decode 兼容：encode(decode(v)) == v。
func encode(key []byte, value []byte, w io.Writer) (int, error) {
	// encoding format:
	// [encoded total length in bytes][encoded key length in bytes][key][value]

	// 计算总长度
	keyLen := len(key)
	valueLen := len(value)
	totalLen := 8 + 8 + keyLen + valueLen // 总长度：encodedLen + keyLen + key + value

	// 序列化数据
	encodedLen := encodeInt(totalLen)
	keyLenBytes := encodeInt(keyLen)

	// 写入数据到 w
	bytesWritten := 0

	// 写入总长度
	if n, err := w.Write(encodedLen); err != nil {
		return n, err
	} else {
		bytesWritten += n
	}

	// 写入 key 长度
	if n, err := w.Write(keyLenBytes); err != nil {
		return bytesWritten + n, err
	} else {
		bytesWritten += n
	}

	// 写入 key
	if n, err := w.Write(key); err != nil {
		return bytesWritten + n, err
	} else {
		bytesWritten += n
	}

	// 写入 value
	if n, err := w.Write(value); err != nil {
		return bytesWritten + n, err
	} else {
		bytesWritten += n
	}

	return bytesWritten, nil
}

// decode 函数通过从指定的读取器读取来解码键和值。
// 返回读取的字节数以及可能发生的错误。
// 该函数必须与 encode 兼容：encode(decode(v)) == v。
func decode(r io.Reader) ([]byte, []byte, error) {
	// encoding format:
	// [encoded total length in bytes][encoded key length in bytes][key][value]

	var encodedEntryLen [8]byte
	if _, err := r.Read(encodedEntryLen[:]); err != nil {
		return nil, nil, err
	}

	entryLen := decodeInt(encodedEntryLen[:])
	encodedEntry := make([]byte, entryLen)
	n, err := r.Read(encodedEntry)
	if err != nil {
		return nil, nil, err
	}

	if n < entryLen {
		return nil, nil, fmt.Errorf("the file is corrupted, failed to read entry")
	}

	keyLen := decodeInt(encodedEntry[0:8])
	key := encodedEntry[8 : 8+keyLen]
	keyPartLen := 8 + keyLen

	if keyPartLen == len(encodedEntry) {
		return key, nil, err
	}

	valueStart := keyPartLen
	value := encodedEntry[valueStart:]

	return key, value, err
}

// encodeKeyOffset encodes key offset and writes it to the given writer.
func encodeKeyOffset(key []byte, offset int, w io.Writer) (int, error) {
	return encode(key, encodeInt(offset), w)
}

// encodeInt encodes the int as a slice of bytes.
// Must be compatible with decodeInt.
func encodeInt(x int) []byte {
	// 采用 sonics 来进行编码，将整数转换成字节
	encoded, _ := sonic.Marshal(x)
	return encoded
}

// decodeInt decodes the slice of bytes as an int.
// Must be compatible with encodeInt.
func decodeInt(encoded []byte) int {
	// 使用 sonics 解码，返回一个整数
	var result int
	err := sonic.Unmarshal(encoded, &result)
	if err != nil {
		panic(fmt.Sprintf("decodeInt failed: %v", err))
	}
	return result
}

// encodeIntPair encodes two ints.
func encodeIntPair(x, y int) []byte {
	// 使用 Sonic 来序列化两个整数
	encoded, _ := sonic.Marshal([2]int{x, y})
	return encoded
}

// decodeIntPair decodes two ints.
func decodeIntPair(encoded []byte) (int, int) {
	// 使用 Sonic 解码两个整数
	var result [2]int
	err := sonic.Unmarshal(encoded, &result)
	if err != nil {
		panic(fmt.Sprintf("decodeIntPair failed: %v", err))
	}
	return result[0], result[1]
}
