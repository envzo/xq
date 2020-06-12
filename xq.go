package xq

import (
	"bytes"
	"errors"
)

type mode int8

const (
	modeNone mode = iota
	modeSelect
	modeUpdate
)

type XSQL struct {
	buf  bytes.Buffer
	mode mode

	t             string
	joinTs        []JoinT
	fields        []string
	sets          []string
	filters       []string
	order         string
	offset, limit int64
	args          []interface{}
}

type JoinT struct {
	t     string
	rules []string
	args  []interface{}
}

type KV map[string]interface{}
type Rule map[string]interface{} // value must be a given variable

func (r Rule) MustCheck() {
	if len(r) != 1 {
		panic(errors.New("rule should only have one pair"))
	}
}

func (r Rule) Unwrap() (string, interface{}) {
	for k, v := range r {
		return k, v
	}
	panic("no data: rule should only have one pair")
}

func New() *XSQL { return &XSQL{mode: modeNone, offset: -1, limit: -1} }

func (x *XSQL) Sel(tb string) *XSQL {
	x.mode = modeSelect
	x.t = tb
	return x
}

func (x *XSQL) Update(tb string) *XSQL {
	x.mode = modeUpdate
	x.t = tb
	return x
}

func (x *XSQL) Cols(f ...string) *XSQL {
	x.fields = append(x.fields, f...)
	return x
}

func (x *XSQL) Set(kv ...KV) *XSQL {
	for _, kv := range kv {
		if len(kv) != 1 {
			panic(errors.New("in Set each kv pair should only have one key"))
		}

		for k, v := range kv {
			x.sets = append(x.sets, k)
			x.args = append(x.args, v)
		}
	}
	return x
}

func (x *XSQL) Join(t string, rule ...Rule) *XSQL {
	jt := JoinT{t: t}

	for _, r := range rule {
		r.MustCheck()

		k, v := r.Unwrap()
		jt.rules = append(jt.rules, k)
		if v != nil {
			jt.args = append(jt.args, v)
		}
	}

	x.joinTs = append(x.joinTs, jt)
	x.args = append(x.args, jt.args...)

	return x
}

func (x *XSQL) Where(f string, arg ...interface{}) *XSQL {
	x.filters = append(x.filters, f)
	x.args = append(x.args, arg...)
	return x
}

func (x *XSQL) WhereOr(rule ...Rule) *XSQL {
	var b bytes.Buffer
	for i, r := range rule {
		r.MustCheck()

		if i > 0 {
			b.WriteString(` or `)
		}

		k, v := r.Unwrap()
		b.WriteString(k)
		if v != nil {
			x.args = append(x.args, v)
		}
	}

	if len(x.fields) > 0 {
		x.filters = append(x.filters, `( `+b.String()+` )`)
	} else {
		x.filters = append(x.filters, b.String())
	}

	return x
}

func (x *XSQL) IfWhere(ok bool, f string, arg ...interface{}) *XSQL {
	if !ok {
		return x
	}
	return x.Where(f, arg...)
}

func (x *XSQL) Order(f string) *XSQL {
	x.order = f
	return x
}

func (x *XSQL) Limit(offset, limit int64) *XSQL {
	if offset < 0 || limit < 0 {
		panic(errors.New("in Limit offset or limit should not less than 0"))
	}
	x.offset, x.limit = offset, limit
	x.args = append(x.args, offset, limit)
	return x
}

func (x *XSQL) Str() string {
	switch x.mode {
	case modeSelect:
		x.buf.WriteString(`select`)

		for i, f := range x.fields {
			if i > 0 {
				x.buf.WriteString(`,`)
			}
			x.buf.WriteString(` ` + f)
		}

		x.buf.WriteString(` from ` + x.t)

		if len(x.joinTs) > 0 {
			x.buf.WriteString(` ` + appendJoin(x.joinTs))
		}
	case modeUpdate:
		x.buf.WriteString(`update ` + x.t)

		if len(x.joinTs) > 0 {
			x.buf.WriteString(` ` + appendJoin(x.joinTs))
		}

		for i, set := range x.sets {
			if i == 0 {
				x.buf.WriteString(` set`)
			} else if i > 0 {
				x.buf.WriteString(`,`)
			}
			x.buf.WriteString(` ` + set + ` = ?`)
		}
	}

	for i, f := range x.filters {
		if i == 0 {
			x.buf.WriteString(` where`)
		} else if i > 0 {
			x.buf.WriteString(` and`)
		}

		x.buf.WriteString(` ` + f)
	}

	if len(x.order) > 0 {
		x.buf.WriteString(` order by ` + x.order)
	}

	if x.offset > -1 || x.limit > -1 {
		x.buf.WriteString(` limit ?, ?`)
	}

	return x.buf.String()
}

func (x *XSQL) Args() []interface{} { return x.args }

func appendJoin(ts []JoinT) string {
	var b bytes.Buffer
	for i, j := range ts {
		if i > 0 {
			b.WriteString(` `)
		}
		b.WriteString(`join ` + j.t)
		for i, r := range j.rules {
			if i == 0 {
				b.WriteString(` on`)
			} else if i > 0 {
				b.WriteString(` and`)
			}
			b.WriteString(` ` + r)
		}
	}
	return b.String()
}
