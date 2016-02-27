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

type InterfaceDefinition struct {
	Alias string
}

type Config struct {
	Global struct {
		Update_Interval_Seconds uint
		Storage_Location        string
		Live_Size               int
		Web_Server_Bind_Address string
		Web_Root                string
	}
	Interface map[string]*struct {
		Alias string
	}
}

func NewConfig(p string) (*Config, error) {
	var c Config
	c.Global.Update_Interval_Seconds = defaultUpdateInterval
	c.Global.Storage_Location = defaultStorageLocation
	c.Global.Live_Size = defaultLiveSize
	c.Global.Web_Server_Bind_Address = defaultBindAddress
	c.Global.Web_Root = defaultWebRoot
	if err := cfg.ReadFileInto(&c, p); err != nil {
		return nil, err
	}
	return &c, nil
}
