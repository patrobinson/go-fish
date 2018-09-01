package main

import (
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
	es "github.com/patrobinson/go-fish/testdata/statefulIntegrationTests/eventStructs"
	log "github.com/sirupsen/logrus"
)

type cloudTrailRule struct {
	State   state.State
	kvStore *state.KVStore
}

func (rule *cloudTrailRule) Init(s ...interface{}) error {
	invalidStateError := fmt.Errorf("This rule expects a KVStore state, but got: %v", s)
	if len(s) < 1 {
		return invalidStateError
	}
	var ok bool
	rule.kvStore, ok = s[0].(*state.KVStore)
	if !ok {
		return invalidStateError
	}
	return rule.kvStore.Init()
}

func (rule *cloudTrailRule) Window() ([]output.OutputEvent, error) {
	return []output.OutputEvent{}, nil
}

func (rule *cloudTrailRule) WindowInterval() int { return 0 }

func (rule *cloudTrailRule) Process(evt interface{}) interface{} {
	cloudTrailEvent, ok := evt.(es.CloudTrail)
	if !ok {
		return nil
	}

	switch cloudTrailEvent.EventName {
	case "AssumeRole":
		err := rule.processAssumeRoleEvent(cloudTrailEvent)
		if err != nil {
			log.Error(err)
		}
		return nil
	case "CreateUser":
		outputEvent := rule.processCreateUserEvent(cloudTrailEvent)
		log.Debugf("Returning event %v", outputEvent)
		return outputEvent
	}

	return nil
}

func (rule *cloudTrailRule) String() string { return "cloudTrailRule" }

func (rule *cloudTrailRule) Close() error {
	rule.kvStore.Close()
	return nil
}

var Rule cloudTrailRule

func (rule *cloudTrailRule) processAssumeRoleEvent(evt es.CloudTrail) error {
	var response es.AssumeRoleResponseElement
	err := mapstructure.Decode(evt.ResponseElements, &response)
	if err != nil {
		return fmt.Errorf("Invalid assume role event: %v", err)
	}
	assumeRoleID := response.AssumedRoleUser.AssumedRoleID
	principal := rule.generatePrincipalName(evt.UserIdentity)

	return rule.kvStore.Set([]byte(assumeRoleID), []byte(principal))
}

func (rule *cloudTrailRule) processCreateUserEvent(evt es.CloudTrail) output.OutputEvent {
	return output.OutputEvent{
		Source:    "CloudTrail",
		EventTime: evt.EventTime,
		EventType: "UserCreated",
		Name:      "IAMUserCreated",
		Level:     output.WarnLevel,
		EventId:   evt.EventID,
		Entity:    rule.generatePrincipalName(evt.UserIdentity),
		SourceIP:  evt.SourceIPAddress,
		Body: map[string]interface{}{
			"AccountID":   evt.RecipientAccountID,
			"UserCreated": evt.RequestParameters["userName"],
		},
		Occurrences: 1,
	}
}

func (rule *cloudTrailRule) generatePrincipalName(userIdentity es.UserIdentity) string {
	switch userIdentity.Type {
	case "IAMUser":
		return fmt.Sprintf("user/%v", userIdentity.UserName)
	case "AWSAccount":
		return fmt.Sprintf("account/%v", userIdentity.AccountID)
	case "AssumedRole":
		return rule.findPrincipalWhoAssumedRole(userIdentity)
	case "FederatedUser":
		return fmt.Sprintf("user/%v", userIdentity.SessionContext.SessionIssuer.UserName)
	case "AWSService":
		return fmt.Sprintf("service/%v", userIdentity.InvokedBy)
	case "Root":
		return findPrincipalRootUser(userIdentity)
	}

	return "unknown"
}

func findPrincipalRootUser(userIdentity es.UserIdentity) string {
	if accountName := userIdentity.UserName; accountName != "" {
		return fmt.Sprintf("account/%v", accountName)
	}
	return fmt.Sprintf("account/%v", userIdentity.AccountID)
}

func (rule *cloudTrailRule) findPrincipalWhoAssumedRole(userIdentity es.UserIdentity) string {
	if userName := string(rule.kvStore.Get([]byte(userIdentity.PrincipalID))); userName != "" {
		return userName
	}

	return userIdentity.PrincipalID
}
