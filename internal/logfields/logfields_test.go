package logfields

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// snakeCase matches lower_snake_case identifiers: lowercase letters/digits,
// words separated by single underscores, no leading/trailing/double
// underscores.
var snakeCase = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// TestKeysAreSnakeCase parses logfields.go and asserts that every exported
// Key* constant's string value is snake_case. This is a parse-the-source
// check (rather than a hardcoded list) so a future addition to the canon
// can't silently reintroduce the camelCase/mixed-case drift this package
// exists to eliminate.
func TestKeysAreSnakeCase(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "logfields.go", nil, 0)
	if err != nil {
		t.Fatalf("parse logfields.go: %v", err)
	}

	checked := 0
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range valueSpec.Names {
				if !strings.HasPrefix(name.Name, "Key") || !name.IsExported() {
					continue
				}
				if i >= len(valueSpec.Values) {
					// const block reusing the previous line's expression
					// (iota-style); none of our Key* consts do this, but
					// skip rather than panic if that ever changes.
					continue
				}
				lit, ok := valueSpec.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					t.Errorf("%s: value is not a string literal", name.Name)
					continue
				}
				val, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Errorf("%s: unquote %s: %v", name.Name, lit.Value, err)
					continue
				}
				checked++
				if !snakeCase.MatchString(val) {
					t.Errorf("%s = %q is not snake_case", name.Name, val)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no exported Key* constants found in logfields.go — test is not exercising anything")
	}
}
