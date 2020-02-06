package bqin

import (
	"os"

	goconfig "github.com/kayac/go-config"
)

type Config struct {
	AWS *AWSConfig `yaml:"aws"`
	GCP *GCPConfig `yaml:"gcp"`
}

type AWSConfig struct {
	Region string `yaml:"region"`
	Queue  string `yaml:"queue"`
}

type GCPConfig struct {
	ProjectID     string `yaml:"project_id"`
	TmpBucket     string `yaml:"tmp_bucket"`
	TargetTable   string `yaml:"target_table"`
	TargetDataset string `yaml:"target_dataset"`
}

func NewDefaultConfig() *Config {
	return &Config{
		AWS: &AWSConfig{
			Region: os.Getenv("AWS_REGION"),
		},
		GCP: &GCPConfig{
			ProjectID: os.Getenv("GCP_PROJECT_ID"),
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	conf := NewDefaultConfig()
	err := goconfig.LoadWithEnv(conf, path)
	return conf, err
}
