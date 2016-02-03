package main

import (
	"errors"

	cfg "gopkg.in/gcfg.v1"
)

var (
	ErrInvalidConfig              = errors.New("Invalid Configuration")
	defaultUpdateInterval  uint   = 1
	defaultStorageLocation string = `/opt/gobwmon/`
	defaultWebRoot         string = `/opt/gobwmon/www/`
	defaultLiveSize        int    = 120
	defaultBindAddress     string = `0.0.0.0:80`
)

type Config struct {
	Interface               []string
	Update_Interval_Seconds uint
	Storage_Location        string
	Live_Size               int
	Web_Server_Bind_Address string
	Web_Root                string
}

type fconfig struct {
	Interface_Config Config
}

func NewConfig(p string) (*Config, error) {
	c := Config{
		Update_Interval_Seconds: defaultUpdateInterval,
		Storage_Location:        defaultStorageLocation,
		Live_Size:               defaultLiveSize,
		Web_Server_Bind_Address: defaultBindAddress,
		Web_Root:                defaultWebRoot,
	}
	fc := fconfig{
		Interface_Config: c,
	}
	if err := cfg.ReadFileInto(&fc, p); err != nil {
		return nil, err
	}
	return &fc.Interface_Config, nil
}
