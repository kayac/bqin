package cloud_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"testing"

	"github.com/kayac/bqin/cloud"
	"golang.org/x/oauth2"
	"google.golang.org/api/transport"
	yaml "gopkg.in/yaml.v2"
)

func TestGCPCredential(t *testing.T) {
	yamlStr := fmt.Sprintf(`
credential: |
  {
    "type": "service_account",
    "project_id": "bqin-test-gcp",
    "private_key_id": "0000000000000000000000000000000000000000",
    "private_key": "%s",
    "client_email": "bqin@bqin-test-gcp.iam.gserviceaccount.com",
    "client_id": "000000000000000000000",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bqin%%40bqin-test-gcp.iam.gserviceaccount.com"
  }
`, strings.Replace(generatePEM(), "\n", "\\n", -1))
	gcp := &cloud.GCP{}
	decoder := yaml.NewDecoder(strings.NewReader(yamlStr))
	if err := decoder.Decode(gcp); err != nil {
		t.Fatalf("unexpected decode error: %s", err)
	}
	opts := gcp.BuildOption("hoge")
	if len(opts) != 1 {
		t.Fatalf("unexpected builded options: %v", opts)
	}
	client, _, err := transport.NewHTTPClient(context.Background(), opts...)
	if err != nil {
		t.Fatalf("unexpected new client error: %s", err)
	}
	trans, ok := client.Transport.(*oauth2.Transport)
	if !ok {
		t.Fatalf("unexpected client transport type: %#v", client)
	}
}

func generatePEM() string {
	rsaPrivateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	derRsaPrivateKey := x509.MarshalPKCS1PrivateKey(rsaPrivateKey)
	buf := new(bytes.Buffer)
	pem.Encode(buf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: derRsaPrivateKey})
	return buf.String()
}
