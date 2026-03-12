package server

import (
	"os"
	"testing"
)

func TestLoadConfig_DefaultPersistPath(t *testing.T) {
	// Clear any env override
	os.Unsetenv("OJS_PERSIST")

	cfg := LoadConfig()
	if cfg.PersistPath != "ojs-amqp.db" {
		t.Errorf("expected default PersistPath 'ojs-amqp.db', got %q", cfg.PersistPath)
	}
}

func TestLoadConfig_CustomPersistPath(t *testing.T) {
	t.Setenv("OJS_PERSIST", "/custom/path/state.db")
	cfg := LoadConfig()
	if cfg.PersistPath != "/custom/path/state.db" {
		t.Errorf("expected PersistPath '/custom/path/state.db', got %q", cfg.PersistPath)
	}
}

func TestLoadConfig_DisabledPersist(t *testing.T) {
	t.Setenv("OJS_PERSIST", "")
	cfg := LoadConfig()
	if cfg.PersistPath != "" {
		t.Errorf("expected empty PersistPath when OJS_PERSIST='', got %q", cfg.PersistPath)
	}
}

func TestLoadConfig_DefaultAMQPURL(t *testing.T) {
	os.Unsetenv("AMQP_URL")
	cfg := LoadConfig()
	if cfg.AMQPURL != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("expected default AMQP URL, got %q", cfg.AMQPURL)
	}
}

func TestLoadConfig_CustomAMQPURL(t *testing.T) {
	t.Setenv("AMQP_URL", "amqp://prod:secret@rabbit.example.com:5672/vhost")
	cfg := LoadConfig()
	if cfg.AMQPURL != "amqp://prod:secret@rabbit.example.com:5672/vhost" {
		t.Errorf("expected custom AMQP URL, got %q", cfg.AMQPURL)
	}
}
