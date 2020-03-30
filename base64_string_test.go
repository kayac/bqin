package bqin_test

import (
	"strings"
	"testing"

	"github.com/kayac/bqin"

	yaml "gopkg.in/yaml.v2"
)

func TestBase64String(t *testing.T) {
	cases := []struct {
		orig     string
		isErr    bool
		expected string
	}{
		{
			orig:  "hoge! DAYO",
			isErr: true,
		},
		{
			orig:  "eyJ0eXBlIjogImhvZ2Vob2dlIn0=",
			isErr: false,
		},
	}
	for _, c := range cases {
		t.Run(c.orig, func(t *testing.T) {
			decoder := yaml.NewDecoder(strings.NewReader(c.orig))
			var b64str bqin.Base64String
			if err := decoder.Decode(&b64str); (err != nil) != c.isErr {
				t.Errorf("unexpected error: %s", err)
				return
			}
			if c.isErr == true {
				return
			}
			if b64str.String() != c.orig {
				t.Logf("     got: %s", b64str.String())
				t.Logf("expected: %s", c.orig)
				t.Error("unexpected")
			}
		})
	}
}
