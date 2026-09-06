package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func Timeout() (time.Duration, error) {
	value := os.Getenv("SMOKE_TIMEOUT")
	if value == "" {
		value = "60s"
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("SMOKE_TIMEOUT must be a positive duration")
	}
	return d, nil
}

func Brokers() ([]string, error) {
	value, err := required("KAFKA_BROKERS")
	if err != nil {
		return nil, err
	}
	brokers := strings.Split(value, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
		host, port, err := net.SplitHostPort(brokers[i])
		if err != nil || host == "" || !validPort(port) {
			return nil, fmt.Errorf("KAFKA_BROKERS requires comma-separated host:port addresses")
		}
	}
	return brokers, nil
}

type MySQL struct {
	Address, Database, User, Password string
}

func Database() (MySQL, error) {
	var c MySQL
	values := make(map[string]string)
	for _, name := range []string{"MYSQL_HOST", "MYSQL_PORT", "MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD"} {
		value, err := required(name)
		if err != nil {
			return c, err
		}
		values[name] = value
	}
	if !validPort(values["MYSQL_PORT"]) {
		return c, fmt.Errorf("MYSQL_PORT must be in 1..65535")
	}
	c.Address = net.JoinHostPort(values["MYSQL_HOST"], values["MYSQL_PORT"])
	c.Database, c.User, c.Password = values["MYSQL_DATABASE"], values["MYSQL_USER"], values["MYSQL_PASSWORD"]
	return c, nil
}

func required(name string) (string, error) {
	value := os.Getenv(name)
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func validPort(value string) bool {
	n, err := strconv.Atoi(value)
	return err == nil && n > 0 && n <= 65535
}
