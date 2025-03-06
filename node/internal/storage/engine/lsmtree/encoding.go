package lsmtree

import (
	"encoding/binary"
	"fmt"
	"io"
)

// encode 对键和值进行编码，并将其写入指定的写入器。
// 返回写入的字节数和发生的错误。
// 此函数必须与 decode 兼容：encode(decode(v)) == v。
func encode(key []byte, value []byte, w io.Writer) (int, error) {
	// 编码格式：
	// [编码的总长度（字节）][编码的键长度（字节）][键][值]

	// 已写入的字节数
	bytes := 0

	keyLen := encodeInt(len(key))
	len := len(keyLen) + len(key) + len(value)
	encodedLen := encodeInt(len)

	if n, err := w.Write(encodedLen); err != nil {
		return n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(keyLen); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(key); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(value); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	return bytes, nil
}

// decode 从指定的读取器中解码键和值。
// 返回读取的字节数和发生的错误。
// 此函数必须与 encode 兼容：encode(decode(v)) == v。
func decode(r io.Reader) ([]byte, []byte, error) {
	// 编码格式：
	// [编码的总长度（字节）][编码的键长度（字节）][键][值]

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

// encodeKeyOffset 编码键偏移量并将其写入给定的写入器。
func encodeKeyOffset(key []byte, offset int, w io.Writer) (int, error) {
	return encode(key, encodeInt(offset), w)
}

// encodeInt 将整数编码为字节切片。
// 必须与 decodeInt 兼容。
func encodeInt(x int) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(x))

	return encoded[:]
}

// decodeInt 将字节切片解码为整数。
// 必须与 encodeInt 兼容。
func decodeInt(encoded []byte) int {
	return int(binary.BigEndian.Uint64(encoded))
}

// encodeIntPair 编码两个整数。
func encodeIntPair(x, y int) []byte {
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[0:8], uint64(x))
	binary.BigEndian.PutUint64(encoded[8:], uint64(y))

	return encoded[:]
}

// decodeIntPair 解码两个整数。
func decodeIntPair(encoded []byte) (int, int) {
	x := int(binary.BigEndian.Uint64(encoded[0:8]))
	y := int(binary.BigEndian.Uint64(encoded[8:]))

	return x, y
}
