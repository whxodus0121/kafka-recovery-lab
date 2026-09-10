package main

import "testing"

func TestParseOptionsKeepsSingleAndBulkModesDistinct(t *testing.T) {
	single, err := parseOptions([]string{"-partition", "0", "-offset", "10"})
	if err != nil || single.startOffset != 10 || single.limit != 1 || single.recoveryTopic != "inventory.recovery.v1" {
		t.Fatal("single mode", single, err)
	}
	bulk, err := parseOptions([]string{"-partition", "0", "-start-offset", "100", "-limit", "120", "-publish-rate", "20"})
	if err != nil || bulk.startOffset != 100 || bulk.limit != 120 || bulk.publishRate != 20 {
		t.Fatal("bulk mode", bulk, err)
	}
	for _, args := range [][]string{
		{"-partition", "0", "-offset", "1", "-start-offset", "1", "-limit", "2"},
		{"-partition", "0", "-start-offset", "1"},
		{"-partition", "0", "-start-offset", "1", "-limit", "0"},
		{"-partition", "0", "-offset", "1", "-publish-rate", "-1"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Fatal("accepted invalid mode", args)
		}
	}
}
