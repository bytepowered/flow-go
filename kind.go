package flow

func SetKindNameMapper(mapper KindNameMapper) {
	_kindNameMapper = mapper
}

type KindNameMapper map[Kind]string

func (m KindNameMapper) GetName(k Kind) string {
	if v, ok := m[k]; ok {
		return v
	}
	return "undefined"
}

func (m KindNameMapper) SetName(k Kind, name string) {
	m[k] = name
}
