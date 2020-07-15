package xq

import (
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	x := T("shop").W("id = ?", 1).Sel("name", "addr", "city")
	t.Log(x.SQL())
}

func TestUpdate(t *testing.T) {
	x := T("shop").W("id = ?", 1).Update("closed", true, "update_dt", time.Now())
	t.Log(x.SQL())
}

func TestJoin(t *testing.T) {
	city := "上海"
	x := T("m_shop m").J("shop s").On("s.mid = m.id").On("s.state = ?", 3).
		W("m.closed = ?", false).XA(len(city) == 0, "m.city = ?", city).
		Order("m.update_dt").Sel("m.id", "m.name", "m.addr", "m.city").Limit(0, 10)
	t.Log(x.SQL())
}
