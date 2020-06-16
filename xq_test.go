package xq

import (
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	x := New().Sel("shop").Cols("name", "addr", "city").Where(XR{R: R{"id = ?": 1}})
	t.Log(x.Str())
}

func TestUpdate(t *testing.T) {
	x := New().Update("shop").Set(
		KV{"closed": true},
		KV{"update_dt": time.Now()},
	).Where(XR{
		R: R{"id = ?": 1},
	})
	t.Log(x.Str())
}

func TestJoin(t *testing.T) {
	city := "上海"
	x := New().Sel("mshop m").Cols("m.id", "m.name", "m.addr", "m.city").Join("shop s",
		R{"s.mid = m.id": nil}, R{"s.state = ?": 3}).
		Where(XR{R: R{"m.closed = ?": false}}).Where(XR{Ignore: len(city) == 0, R: R{"m.city = ?": city}}).
		Order("m.update_dt").Limit(0, 10)
	t.Log(x.Str())
}

func TestWhereOr(t *testing.T) {
	x := New().Sel(`shop`).Cols(`id`, `name`).WhereOr(XR{Ignore: true, R: R{`last_raw_dt is null`: nil}}, XR{Ignore: false, R: R{`last_raw_dt < last_search_dt`: nil}}).
		Where(XR{R: R{`last_search_dt is not null`: nil}}).Order(`last_search_dt asc`).Limit(0, 1)
	t.Log(x.Str())
}
