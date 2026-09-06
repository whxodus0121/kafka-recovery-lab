package config

import (
	"strings"
	"testing"
)

func TestBrokersRejectMalformedEndpoints(t *testing.T) {
	for _, value := range []string{"", "localhost", "host:0", "host:65536", "host:abc", "host:9092,", ":9092"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("KAFKA_BROKERS", value)
			if _, err := Brokers(); err == nil {
				t.Fatal("invalid endpoint accepted")
			}
		})
	}
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092, [::1]:9093")
	got, err := Brokers()
	if err != nil || len(got) != 2 || got[1] != "[::1]:9093" {
		t.Fatalf("valid endpoints rejected: %v %v", got, err)
	}
}

func TestDatabaseValidationDoesNotLeakPassword(t *testing.T) {
	for name, value := range map[string]string{"MYSQL_HOST": "localhost", "MYSQL_PORT": "3306", "MYSQL_DATABASE": "lab", "MYSQL_USER": "lab", "MYSQL_PASSWORD": "private-test-marker"} {
		t.Setenv(name, value)
	}
	if _, err := Database(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYSQL_PORT", "70000")
	if _, err := Database(); err == nil || strings.Contains(err.Error(), "private-test-marker") {
		t.Fatal("invalid port accepted or password leaked")
	}
	t.Setenv("MYSQL_PASSWORD", "")
	if _, err := Database(); err == nil {
		t.Fatal("empty password accepted")
	}
}

func TestTimeoutRejectsUnboundedValues(t *testing.T) {
	for _, value := range []string{"0s", "-1s", "invalid"} {
		t.Setenv("SMOKE_TIMEOUT", value)
		if _, err := Timeout(); err == nil {
			t.Fatal("invalid timeout accepted")
		}
	}
	t.Setenv("SMOKE_TIMEOUT", "")
	if d, err := Timeout(); err != nil || d <= 0 {
		t.Fatal("missing timeout must use a bounded default")
	}
}
