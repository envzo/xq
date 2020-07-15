package xq

import "bytes"

type P struct {
	b bytes.Buffer
}

func (p *P) Write(s ...string) *P {
	for _, s := range s {
		if len(s) == 0 {
			continue
		}
		if p.b.Len() != 0 {
			p.b.WriteString(` `)
		}
		p.b.WriteString(s)
	}
	return p
}

func (p *P) Quote(s ...string) *P {
	return p.Write(`(`).Write(s...).Write(`)`)
}

func (p *P) Str() string { return p.b.String() }
