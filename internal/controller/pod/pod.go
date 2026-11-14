package pod

type Pod struct {
	name string
	ip   string
}

func NewPod(name string, ip string) *Pod {
	return &Pod{
		name: name, 
		ip: ip,
	}
}

func (p *Pod) Name() string {
	return p.name
}

func (p *Pod) IP() string {
	return p.ip
}