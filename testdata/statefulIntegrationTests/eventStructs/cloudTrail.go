package eventStructs

import "time"

const CloudTrailEventName = "CloudTrail"

type CloudTrail struct {
	EventVersion      string       `json:"eventVersion"`
	UserIdentity      UserIdentity `json:"userIdentity"`
	EventTime         time.Time    `json:"eventTime"`
	EventSource       string       `json:"eventSource"`
	EventName         string       `json:"eventName"`
	AwsRegion         string       `json:"awsRegion"`
	SourceIPAddress   string       `json:"sourceIPAddress"`
	UserAgent         string       `json:"userAgent"`
	RequestParameters map[string]interface{}
	ResponseElements  ResponseElements
	Resources         []struct {
		ARN       string `json:"ARN"`
		AccountID string `json:"accountId"`
		Type      string `json:"type"`
	} `json:"resources"`
	RequestID          string `json:"requestID"`
	SharedEventID      string `json:"sharedEventID"`
	EventID            string `json:"eventID"`
	EventType          string `json:"eventType"`
	RecipientAccountID string `json:"recipientAccountId"`
}

type UserIdentity struct {
	Type           string `json:"type"`
	PrincipalID    string `json:"principalId"`
	Arn            string `json:"arn,omitempty"`
	AccountID      string `json:"accountId"`
	AccessKeyID    string `json:"accessKeyId,omitempty"`
	UserName       string `json:"userName,omitempty"`
	InvokedBy      string `json:"invokedBy,omitempty"`
	SessionContext struct {
		Attributes struct {
			MfaAuthenticated string    `json:"mfaAuthenticated"`
			CreationDate     time.Time `json:"creationDate"`
		} `json:"attributes"`
		SessionIssuer struct {
			Type        string `json:"type"`
			PrincipalID string `json:"principalId"`
			Arn         string `json:"arn"`
			AccountID   string `json:"accountId"`
			UserName    string `json:"userName,omitempty"`
		} `json:"sessionIssuer"`
	} `json:"sessionContext,omitempty"`
}

func (e CloudTrail) TypeName() string {
	return CloudTrailEventName
}

type ResponseElements interface{}

type AssumeRoleResponseElement struct {
	Credentials struct {
		SessionToken string `json:"sessionToken"`
		AccessKeyID  string `json:"accessKeyId"`
		Expiration   string `json:"expiration"`
	} `json:"credentials"`
	AssumedRoleUser struct {
		AssumedRoleID string `json:"assumedRoleId"`
		Arn           string `json:"arn"`
	} `json:"assumedRoleUser"`
}
