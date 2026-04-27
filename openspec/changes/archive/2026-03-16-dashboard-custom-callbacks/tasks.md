## 1. Registration Form Update

- [x] 1.1 In `templates/home.html`, change the callback URL input from `readonly` to editable, keeping the default value pre-filled with `{{.CallbackURL}}`
- [x] 1.2 Add `name="callbackUrl"` attribute to the callback URL input so it's submitted with the form

## 2. Handler Update

- [x] 2.1 In `handleRegister`, read `callbackUrl` from `r.FormValue("callbackUrl")` instead of using the hardcoded `h.callbackURL`
- [x] 2.2 Add validation: if callback URL is empty, render an error flash message
- [x] 2.3 Use the user-provided callback URL in the JSON payload POSTed to merkle-service `/watch`
- [x] 2.4 Pass the user-provided callback URL (not `h.callbackURL`) to `txidTracker.Add()`
- [x] 2.5 Update the success flash message to show the actual registered callback URL

## 3. Tests

- [x] 3.1 Add test for `handleRegister` with custom callback URL — verify the correct URL is sent to merkle-service
- [x] 3.2 Add test for `handleRegister` with empty callback URL — verify error flash is returned
- [x] 3.3 Add test for `handleRegister` with default callback URL — verify backward-compatible behavior

## 4. Build Verification

- [x] 4.1 Run `go build ./tools/debug-dashboard` to verify compilation
- [x] 4.2 Run `go vet ./tools/debug-dashboard/...` to check for issues
- [x] 4.3 Run `go test ./tools/debug-dashboard/...` to verify all tests pass
