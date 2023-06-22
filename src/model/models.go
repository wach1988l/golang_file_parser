package model

import "encoding/json"

type CSVData struct {
	NumId string `json:"-"`

	Id string `json:"id"`

	Price float64 `json:"price"`

	ExpireDate string `json:"expiration_date"`
}

func (data *CSVData) MarshalToBinary() ([]byte, error) {
	return json.Marshal(data)
}
