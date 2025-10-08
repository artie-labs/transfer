package config

func (c *Config) IsStagingTableReuseEnabled() bool {
	return c.StagingTableReuse != nil && c.StagingTableReuse.Enabled
}

func (c *Config) GetStagingTableSuffix() string {
	if c.StagingTableReuse != nil && c.StagingTableReuse.TableNameSuffix != "" {
		return c.StagingTableReuse.TableNameSuffix
	}
	return "_staging" // default suffix
}

func (c *Config) GetStagingTableReuseConfig() *StagingTableReuseConfig {
	return c.StagingTableReuse
}
