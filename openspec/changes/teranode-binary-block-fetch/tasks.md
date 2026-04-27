## 1. Binary Block Parser

- [x] 1.1 Add `ParseBinaryBlockMetadata(data []byte) (*BlockMetadata, error)` to `internal/datahub/client.go`: read 4-byte uint32 LE height, 4-byte uint32 LE subtree count, then N×32-byte subtree hashes into hex-encoded strings
- [x] 1.2 Add length validation guard: return error if `len(data) < 8` or `(len(data)-8) % 32 != 0` or actual byte count doesn't match declared subtree count

## 2. FetchBlockMetadata Update

- [x] 2.1 Update `FetchBlockMetadata` in `internal/datahub/client.go` to call `/block/{hash}` instead of `/block/{hash}/json`
- [x] 2.2 Replace `json.Unmarshal` call with `ParseBinaryBlockMetadata` call
- [x] 2.3 Remove `encoding/json` import if no longer used elsewhere in the file

## 3. Tests

- [x] 3.1 Add `TestParseBinaryBlockMetadata_Success`: build a valid binary payload (height=100, 3 subtree hashes) and verify parsed output
- [x] 3.2 Add `TestParseBinaryBlockMetadata_EmptySubtrees`: payload with subtree count = 0, verify empty `Subtrees` slice
- [x] 3.3 Add `TestParseBinaryBlockMetadata_TooShort`: payload shorter than 8 bytes, verify error
- [x] 3.4 Add `TestParseBinaryBlockMetadata_LengthMismatch`: subtree count declares N but payload only has N-1 hashes, verify error
- [x] 3.5 Update `TestFetchBlockMetadata_Success` to serve a binary response and assert parsed fields

## 4. Validation

- [x] 4.1 Run `go test ./internal/datahub/...` and confirm all tests pass
- [x] 4.2 Run `go test ./...` and confirm no regressions
