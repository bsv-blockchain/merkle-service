package nodes

import (
	"fmt"

	"github.com/bsv-blockchain/teranode/model"
)

// ParsePrevHash extracts the previous-block hash from a hex-encoded 80-byte
// block header as carried on kafka.BlockMessage.Header. Returns empty string
// when headerHex is empty (caller may still attribute by height alone).
func ParsePrevHash(headerHex string) (string, error) {
	if headerHex == "" {
		return "", nil
	}
	h, err := model.NewBlockHeaderFromString(headerHex)
	if err != nil {
		return "", fmt.Errorf("parse block header: %w", err)
	}
	if h.HashPrevBlock == nil {
		return "", nil
	}
	return h.HashPrevBlock.String(), nil
}
