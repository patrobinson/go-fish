package main

import (
	"encoding/json"
	"fmt"

	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
	es "github.com/patrobinson/go-fish/testdata/statefulIntegrationTests/eventStructs"
)

type cloudTrailAggRule struct {
	kvStore state.KVStore
}

func (rule *cloudTrailAggRule) Init() {
	rule.kvStore = state.KVStore{
		DbFileName: "aggregateEvent",
		BucketName: "CloudTrail",
	}
	rule.kvStore.Init()
}

func (rule *cloudTrailAggRule) Process(evt interface{}) interface{} {
	cloudTrailEvent, ok := evt.(es.CloudTrail)
	if !ok {
		return false
	}

	var occurrences int
	if cloudTrailEvent.UserIdentity.SessionContext.Attributes.MfaAuthenticated == "false" {
		var event output.OutputEvent
		principalName := rule.generatePrincipalName(cloudTrailEvent.UserIdentity)
		rawEvt := rule.kvStore.Get([]byte(principalName))
		if rawEvt == nil {
			event = output.OutputEvent{
				Source:    "CloudTrail",
				EventTime: cloudTrailEvent.EventTime,
				EventType: "NoMFA",
				Name:      "NoMFA",
				Level:     output.WarnLevel,
				EventId:   cloudTrailEvent.EventID,
				Entity:    principalName,
				SourceIP:  cloudTrailEvent.SourceIPAddress,
				Body: map[string]interface{}{
					"AccountID": cloudTrailEvent.RecipientAccountID,
				},
				Occurrences: 1,
			}
		} else {
			err := json.Unmarshal(rawEvt, &event)
			if err != nil {
				return err
			}

			event.Occurrences++
		}

		occurrences = event.Occurrences
		rawEvt, err := json.Marshal(event)
		if err != nil {
			return err
		}
		rule.kvStore.Set([]byte(principalName), rawEvt)
	}

	return occurrences
}

func (rule *cloudTrailAggRule) WindowInterval() int {
	return 2
}

func (rule *cloudTrailAggRule) Window() ([]output.OutputEvent, error) {
	var result []output.OutputEvent
	var acc [][]byte
	rule.kvStore.ForEach(func(k, v []byte) error {
		acc = append(acc, v)
		return nil
	})
	for _, v := range acc {
		var event output.OutputEvent
		err := json.Unmarshal(v, &event)
		if err != nil {
			return result, err
		}

		result = append(result, event)
	}
	return result, nil
}

func (rule *cloudTrailAggRule) generatePrincipalName(userIdentity es.UserIdentity) string {
	switch userIdentity.Type {
	case "IAMUser":
		return fmt.Sprintf("user/%v", userIdentity.UserName)
	case "AWSAccount":
		return fmt.Sprintf("account/%v", userIdentity.AccountID)
	case "AssumedRole":
		return fmt.Sprintf("role/%v", userIdentity.SessionContext.SessionIssuer.UserName)
	case "FederatedUser":
		return fmt.Sprintf("user/%v", userIdentity.SessionContext.SessionIssuer.UserName)
	case "AWSService":
		return fmt.Sprintf("service/%v", userIdentity.InvokedBy)
	case "Root":
		return "Root"
	}

	return "unknown"
}

func (rule *cloudTrailAggRule) String() string { return "cloudTrailAggRule" }

func (rule *cloudTrailAggRule) Close() {
	rule.kvStore.Close()
}

var Rule cloudTrailAggRule
