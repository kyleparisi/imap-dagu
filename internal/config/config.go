package config

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Account  string `toml:"account"`
	StateDir string `toml:"state_dir"`
	Backfill bool   `toml:"backfill"`
	IMAP     IMAP   `toml:"imap"`
	Dagu     Dagu   `toml:"dagu"`
}
type IMAP struct {
	Host        string `toml:"host"`
	Port        int    `toml:"port"`
	Username    string `toml:"username"`
	PasswordEnv string `toml:"password_env"`
}
type Dagu struct {
	Bin  string `toml:"bin"`
	DAG  string `toml:"dag"`
	Home string `toml:"home"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

func Load(path string) (Config, error) {
	var c Config
	md, err := toml.DecodeFile(path, &c)
	if err != nil {
		return c, fmt.Errorf("read config: %w", err)
	}
	if len(md.Undecoded()) != 0 {
		return c, fmt.Errorf("unknown configuration keys: %v", md.Undecoded())
	}
	if c.IMAP.Port == 0 {
		c.IMAP.Port = 993
	}
	if c.Dagu.Bin == "" {
		c.Dagu.Bin = "dagu"
	}
	if !identifier.MatchString(c.Account) || !identifier.MatchString(c.Dagu.DAG) {
		return c, fmt.Errorf("account and dagu.dag must be names using letters, digits, - or _ (starting with a letter or digit)")
	}
	if !filepath.IsAbs(c.StateDir) {
		return c, fmt.Errorf("state_dir must be an absolute path")
	}
	if c.Dagu.Home != "" && !filepath.IsAbs(c.Dagu.Home) {
		return c, fmt.Errorf("dagu.home must be an absolute path")
	}
	if c.IMAP.Host == "" || c.IMAP.Username == "" || c.IMAP.PasswordEnv == "" || c.IMAP.Port < 1 || c.IMAP.Port > 65535 {
		return c, fmt.Errorf("imap requires host, username, password_env, and a valid port")
	}
	return c, nil
}

// Source binds a cursor to its mailbox and destination. Password rotation is safe.
func (c Config) Source() string {
	b, _ := json.Marshal([]any{c.Account, c.IMAP.Host, c.IMAP.Port, c.IMAP.Username, "INBOX", c.Dagu.DAG, c.Dagu.Home})
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
