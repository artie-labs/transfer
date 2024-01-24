package datadog

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func getTags(tags interface{}) []string {
	// Yaml parses lists as a sequence, so we'll unpack it again with the same library.
	if tags == nil {
		return []string{}
	}

	yamlBytes, err := yaml.Marshal(tags)
	if err != nil {
		return []string{}
	}

	var retTagStrings []string
	err = yaml.Unmarshal(yamlBytes, &retTagStrings)
	if err != nil {
		return []string{}
	}

	return retTagStrings
}

func toDatadogTags(tags map[string]string) []string {
	var retTags []string
	for key, val := range tags {
		retTags = append(retTags, fmt.Sprintf("%s:%s", key, val))
	}

	return retTags
}
