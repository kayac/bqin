package bqin

import (
	"github.com/kayac/bqin/cloud"
	goconfig "github.com/kayac/go-config"
	"github.com/pkg/errors"
)

type Config struct {
	QueueName string        `yaml:"queue_name"`
	Cloud     *cloud.Config `yaml:"cloud"`

	Rules []*Rule `yaml:"rules"`
	Rule  `yaml:",inline"`
}

func LoadConfig(path string) (*Config, error) {
	conf := &Config{
		Cloud: cloud.NewDefaultConfig(),
	}
	err := goconfig.LoadWithEnv(conf, path)
	if err != nil {
		return nil, err
	}
	if err := conf.Validate(); err != nil {
		return nil, err
	}
	return conf, nil
}

func (c *Config) Validate() error {
	if c.QueueName == "" {
		return errors.New("queue_name is not defined")
	}
	if len(c.Rules) == 0 {
		return errors.New("rules is not defined")
	}
	for i, other := range c.Rules {
		dst := c.Rule.Clone()
		dst.MergeIn(other)
		if err := dst.Validate(); err != nil {
			return errors.Wrapf(err, "rule[%d]", i)
		}
		c.Rules[i] = dst
	}
	return nil
}
