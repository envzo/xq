package xq

import (
	"strings"
)

type Xq struct {
	state xqState // finite state

	t             *Table
	j             []*Join
	where         []*Exp
	cols          []string
	set           []*Exp
	groupBy       []string
	orderBy       []string
	offset, limit int64
}

type xqState int8

const (
	stateWhere  xqState = 1
	stateJoin   xqState = 2
	stateSel    xqState = 3
	stateUpdate xqState = 4
)

type Table struct {
	Name, Alias string
}

func newTable(n ...string) *Table {
	s := len(n)
	if s != 1 && s != 2 {
		panic("invalid args")
	}
	t := Table{Name: n[0]}
	if s == 2 {
		t.Alias = n[1]
	}
	return &t
}

type Join struct {
	Type int // I L R
	T    *Table
	On   []*Exp
}

type Exp struct {
	t    expType
	stmt string // could be a expression or a col
	args []interface{}
	q    *Xq
}

func (exp *Exp) SQL() string {
	var p P
	switch exp.t {
	case expTypeEq:
		p.Write(exp.stmt)
	case expTypeIn:
		p.Write(exp.stmt, `in`).Write(`(`)
		for j := range exp.args {
			if j > 0 {
				p.Write(`,`)
			}
			p.Write(`?`)
		}
		p.Write(`)`)
	case expTypeInQ:
		p.Write(exp.stmt, `in`).Quote(exp.q.SQL())
	case expTypeNInQ:
		p.Write(exp.stmt, `not in`).Quote(exp.q.SQL())
	}
	return p.Str()
}

func (exp *Exp) Args() []interface{} {
	var args []interface{}
	switch exp.t {
	case expTypeEq, expTypeIn:
		args = append(args, exp.args...)
	case expTypeInQ, expTypeNInQ:
		args = append(args, exp.q.Args()...)
	}
	return args
}

type expType int8

const (
	expTypeEq   expType = 0 // default
	expTypeIn   expType = 1
	expTypeInQ  expType = 2
	expTypeNInQ expType = 3
)

type S struct {
	N bool
	S string
}

func T(tb ...string) *Xq {
	return &Xq{t: newTable(tb...)}
}

func (xq *Xq) J(tb ...string) *Xq {
	return xq.join(&Join{Type: 'I', T: newTable(tb...)})
}

func (xq *Xq) LJ(tb ...string) *Xq {
	return xq.join(&Join{Type: 'L', T: newTable(tb...)})
}

func (xq *Xq) RJ(tb ...string) *Xq {
	return xq.join(&Join{Type: 'R', T: newTable(tb...)})
}

func (xq *Xq) join(j *Join) *Xq {
	xq.state = stateJoin // transfer state
	xq.j = append(xq.j, j)
	return xq
}

func (xq *Xq) On(stmt string, args ...interface{}) *Xq {
	j := xq.j[len(xq.j)-1] // pop the last join
	j.On = append(j.On, &Exp{t: expTypeEq, stmt: stmt, args: args})
	return xq
}

func (xq *Xq) XOn(ok bool, stmt string, args ...interface{}) *Xq {
	if !ok {
		return xq
	}
	return xq.On(stmt, args...)
}

func (xq *Xq) InI64(col string, args []int64) *Xq {
	var s []interface{}
	for _, arg := range args {
		s = append(s, arg)
	}
	return xq.in(col, s...)
}

func (xq *Xq) in(col string, args ...interface{}) *Xq {
	if len(args) == 0 {
		panic("invalid args")
	}
	switch xq.state {
	case stateWhere:
		xq.where = append(xq.where, &Exp{t: expTypeIn, stmt: col, args: args})
	case stateJoin:
		j := xq.j[len(xq.j)-1] // pop the last join
		j.On = append(j.On, &Exp{t: expTypeIn, stmt: col, args: args})
	}
	return xq
}

func (xq *Xq) NInQ(col string, q *Xq) *Xq {
	switch xq.state {
	case stateWhere:
		xq.where = append(xq.where, &Exp{t: expTypeNInQ, stmt: col, q: q})
	}
	return xq
}

func (xq *Xq) InQ(col string, q *Xq) *Xq {
	switch xq.state {
	case stateWhere:
		xq.where = append(xq.where, &Exp{t: expTypeInQ, stmt: col, q: q})
	}
	return xq
}

// Where
func (xq *Xq) W(stmt string, args ...interface{}) *Xq {
	xq.state = stateWhere
	xq.where = append(xq.where, &Exp{t: expTypeEq, stmt: stmt, args: args})
	return xq
}

func (xq *Xq) XW(ok bool, stmt string, args ...interface{}) *Xq {
	if !ok {
		return xq
	}
	return xq.W(stmt, args...)
}

// And
func (xq *Xq) A(stmt string, args ...interface{}) *Xq {
	switch xq.state {
	case stateWhere:
		xq.where = append(xq.where, &Exp{t: expTypeEq, stmt: stmt, args: args})
	}
	return xq
}

func (xq *Xq) XA(ok bool, stmt string, args ...interface{}) *Xq {
	if !ok {
		return xq
	}
	return xq.A(stmt, args...)
}

func (xq *Xq) Sel(col ...string) *Xq {
	xq.state = stateSel
	xq.cols = xq.cols[0:0] // clear
	xq.cols = append(xq.cols, col...)
	return xq
}

func (xq *Xq) Update(pair ...interface{}) *Xq {
	if len(pair) == 0 || len(pair)%2 != 0 {
		panic("invalid args")
	}
	xq.state = stateUpdate
	var exp *Exp
	for i, p := range pair {
		if i%2 == 0 {
			exp = &Exp{t: expTypeEq, stmt: p.(string)}
			continue
		}
		exp.args = []interface{}{p}
		xq.set = append(xq.set, exp)
	}
	return xq
}

// Group
func (xq *Xq) G(col ...string) *Xq {
	xq.groupBy = append(xq.groupBy, col...)
	return xq
}

func (xq *Xq) Order(col ...string) *Xq {
	xq.orderBy = append(xq.orderBy, col...)
	return xq
}

func (xq *Xq) Limit(offset, limit int64) *Xq {
	xq.offset, xq.limit = offset, limit
	return xq
}

func (xq *Xq) Count() *Xq { return xq.Sel(`count(1)`) }

func (xq *Xq) SQL() string {
	switch xq.state {
	case stateSel:
		var p P
		p.Write(`select`).Write(strings.Join(xq.cols, `, `))
		p.Write(`from`, xq.t.Name, xq.t.Alias)

		for _, j := range xq.j { // join
			if len(j.On) == 0 {
				continue
			}
			switch j.Type {
			case 'L':
				p.Write(`left join`)
			case 'R':
				p.Write(`right join`)
			default:
				p.Write(`join`)
			}
			p.Write(j.T.Name, j.T.Alias, `on`)
			for i, on := range j.On {
				if i > 0 {
					p.Write(`and`)
				}
				p.Quote(on.SQL())
			}
		}
		for i, w := range xq.where { // where
			if i == 0 {
				p.Write(`where`)
			} else {
				p.Write(`and`)
			}
			p.Quote(w.SQL())
		}
		if len(xq.groupBy) > 0 {
			p.Write(`group by`, strings.Join(xq.groupBy, `,`))
		}
		if len(xq.orderBy) > 0 {
			p.Write(`order by`, strings.Join(xq.orderBy, `,`))
		}
		if xq.offset > 0 || xq.limit > 0 {
			p.Write(`limit ?, ?`)
		}
		return p.Str()
	case stateUpdate:
		var p P
		p.Write(`update`).Write(xq.t.Name, xq.t.Alias)

		for i, exp := range xq.set {
			if i == 0 {
				p.Write(`set`)
			} else if i > 0 {
				p.Write(`,`)
			}
			p.Write(exp.stmt, `= ?`)
		}
		for i, w := range xq.where { // where
			if i == 0 {
				p.Write(`where`)
			} else {
				p.Write(`and`)
			}
			p.Quote(w.SQL())
		}
		return p.Str()
	default:
		panic("not support yet")
	}
}

func (xq *Xq) Args() []interface{} {
	var args []interface{}
	switch xq.state {
	case stateSel:
		for _, j := range xq.j { // join
			for _, on := range j.On {
				args = append(args, on.Args()...)
			}
		}
		for _, w := range xq.where { // where
			args = append(args, w.Args()...)
		}
		if xq.offset > 0 || xq.limit > 0 {
			args = append(args, xq.offset, xq.limit)
		}
		return args
	case stateUpdate:
		for _, exp := range xq.set {
			args = append(args, exp.args...)
		}
		for _, w := range xq.where { // where
			args = append(args, w.Args()...)
		}
		return args
	default:
		panic("not support yet")
	}
}
