package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Values map[string]map[string]interface{}
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	raw := make(map[string]map[string]interface{})

	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	return &Config{
		Values: raw,
	}, nil
}
