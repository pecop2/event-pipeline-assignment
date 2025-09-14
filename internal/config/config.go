package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBUser           string
	DBPassword       string
	DBHost           string
	DBPort           string
	DBName           string
	WorkerCount      int
	QueueSize        int
	MaxRetries       int
	RetryBaseBackoff time.Duration
}

func Load() *Config {
	return &Config{
		DBUser:           getEnv("MYSQL_USER", "root"),
		DBPassword:       getEnv("MYSQL_ROOT_PASSWORD", "testpass"),
		DBHost:           getEnv("MYSQL_HOST", "localhost"),
		DBPort:           getEnv("MYSQL_PORT", "3306"),
		DBName:           getEnv("MYSQL_DATABASE", "eventdb"),
		WorkerCount:      getEnvInt("WORKER_COUNT", 4),
		QueueSize:        getEnvInt("QUEUE_SIZE", 1000),
		MaxRetries:       getEnvInt("MAX_RETRIES", 3),
		RetryBaseBackoff: getEnvDuration("RETRY_BASE_BACKOFF_MS", 20*time.Millisecond),
	}
}

func (c *Config) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return time.Duration(i) * time.Millisecond
		}
	}
	return fallback
}
