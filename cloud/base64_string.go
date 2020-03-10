package cloud

import "encoding/base64"

type Base64String []byte

func (s Base64String) String() string {
	if s.IsEmpty() {
		return ""
	}
	return base64.StdEncoding.EncodeToString(s)
}

func (s Base64String) IsEmpty() bool {
	return s == nil || len(s) == 0
}

func (s *Base64String) UnmarshalYAML(unmarshal func(interface{}) error) (err error) {
	var str string
	if err = unmarshal(&str); err != nil {
		return
	}
	*s, err = base64.StdEncoding.DecodeString(str)
	return
}

func (s Base64String) Bytes() []byte {
	return []byte(s)
}
