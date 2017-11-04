package eventStructs

const CertStreamEventName = "certStream"

// CertStream taken from https://certstream.calidog.io/
type CertStream struct {
	MessageType string         `json:"message_type"`
	Timestamp   float64        `json:"timestamp,omitempty"`
	Data        CertStreamData `json:"data,omitempty"`
}

// CertStreamData taken from example https://certstream.calidog.io/example.json
type CertStreamData struct {
	UpdateType string `json:"update_type"`
	LeafCert   struct {
		Subject struct {
			Aggregated string      `json:"aggregated"`
			C          interface{} `json:"C"`
			ST         interface{} `json:"ST"`
			L          interface{} `json:"L"`
			O          interface{} `json:"O"`
			OU         interface{} `json:"OU"`
			CN         string      `json:"CN"`
		} `json:"subject"`
		Extensions struct {
			KeyUsage               string `json:"keyUsage"`
			ExtendedKeyUsage       string `json:"extendedKeyUsage"`
			BasicConstraints       string `json:"basicConstraints"`
			SubjectKeyIdentifier   string `json:"subjectKeyIdentifier"`
			AuthorityKeyIdentifier string `json:"authorityKeyIdentifier"`
			AuthorityInfoAccess    string `json:"authorityInfoAccess"`
			SubjectAltName         string `json:"subjectAltName"`
			CertificatePolicies    string `json:"certificatePolicies"`
		} `json:"extensions"`
		NotBefore  float64  `json:"not_before"`
		NotAfter   float64  `json:"not_after"`
		AsDer      string   `json:"as_der"`
		AllDomains []string `json:"all_domains"`
	} `json:"leaf_cert"`
	Chain []struct {
		Subject struct {
			Aggregated string      `json:"aggregated"`
			C          string      `json:"C"`
			ST         interface{} `json:"ST"`
			L          interface{} `json:"L"`
			O          string      `json:"O"`
			OU         interface{} `json:"OU"`
			CN         string      `json:"CN"`
		} `json:"subject"`
		Extensions struct {
			BasicConstraints       string `json:"basicConstraints"`
			KeyUsage               string `json:"keyUsage"`
			AuthorityInfoAccess    string `json:"authorityInfoAccess"`
			AuthorityKeyIdentifier string `json:"authorityKeyIdentifier"`
			CertificatePolicies    string `json:"certificatePolicies"`
			CrlDistributionPoints  string `json:"crlDistributionPoints"`
			SubjectKeyIdentifier   string `json:"subjectKeyIdentifier"`
		} `json:"extensions"`
		NotBefore float64 `json:"not_before"`
		NotAfter  float64 `json:"not_after"`
		AsDer     string  `json:"as_der"`
	} `json:"chain"`
	CertIndex int     `json:"cert_index"`
	Seen      float64 `json:"seen"`
	Source    struct {
		URL  string `json:"url"`
		Name string `json:"name"`
	} `json:"source"`
}

func (e CertStream) TypeName() string {
	return CertStreamEventName
}
