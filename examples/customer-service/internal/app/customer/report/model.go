package report

type Customer struct {
	ID          string `json:"ID"`
	Name        string `json:"Name"`
	Birthdate   string `json:"Birthdate"`
	Sex         byte   `json:"Sex"`
	State       string `json:"State"`
	OnboardedAt string `json:"OnboardedAt"`
}