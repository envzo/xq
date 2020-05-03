package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

const (
	DataTypeInteger  = "integer"
	DataTypeBigint   = "bigint"
	DataTypeVarchar  = "varchar"
	DataTypeBool     = "bool"
	DataTypeDatetime = "datetime"
	DataTypeDouble   = "double"
	DataTypeText     = "text"
)

var model = flag.String("model", "", "model file (YAML)")

type parser struct {
	Tables []*Table
}

func (p *parser) parse(raw yaml.MapSlice) error {
	parse := func(raw yaml.MapSlice, f func(s yaml.MapItem) error) error {
		for _, s := range raw {
			attrs := s.Value.(yaml.MapSlice)
			if len(attrs) == 0 {
				continue
			}

			if err := f(s); err != nil {
				return err
			}
		}
		return nil
	}

	// after all other types parsed, parse table
	if err := parse(raw, func(s yaml.MapItem) error {
		table, err := p.parseTable(s)
		if err != nil {
			return err
		}
		p.Tables = append(p.Tables, table)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (p *parser) parseTable(s yaml.MapItem) (*Table, error) {
	t := Table{
		Name: s.Key.(string),
	}
	for _, attr := range s.Value.(yaml.MapSlice) {
		switch n := attr.Key.(string); n {
		case "db":
			t.DB = attr.Value.(string)
		case "comment":
			t.Comment = attr.Value.(string)
		case "fields":
			if attr.Value == nil {
				continue
			}

			for _, field := range attr.Value.([]interface{}) {
				f, err := p.parseField(field)
				if err != nil {
					return nil, err
				}

				t.Fields = append(t.Fields, f)
			}
		case "uniques":
			uniques, err := p.parseIndexes(t.Fields, attr.Value)
			if err != nil {
				return nil, errors.New("uniques: " + err.Error())
			}
			t.Uniques = uniques
		case "indexes":
			indexes, err := p.parseIndexes(t.Fields, attr.Value)
			if err != nil {
				return nil, errors.New("indexes: " + err.Error())
			}
			t.Uniques = indexes
		}
	}

	// validation
	if t.DB == "" {
		return nil, fmt.Errorf("%s: db should be provided", t.Name)
	}

	return &t, nil
}

func (p *parser) parseIndexes(fields []*Field, in interface{}) ([]Index, error) {
	if in == nil {
		return nil, nil
	}

	m := map[string]struct{}{}
	for _, f := range fields {
		m[f.Name] = struct{}{}
	}

	var indexes []Index
	for _, val := range in.([]interface{}) {
		var index Index
		for _, idx := range val.([]interface{}) {
			col := idx.(string)
			if _, ok := m[col]; !ok { // field not exists
				return nil, fmt.Errorf("field %s not found", col)
			}
			index = append(index, col)
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func (p *parser) parseField(in interface{}) (*Field, error) {
	m := in.(yaml.MapSlice)

	name := m[0].Key.(string)
	if m[0].Value == nil {
		return nil, fmt.Errorf("field '%s' should have type", name)
	}

	t, err := p.parseDatatype(m[0].Value.(string))
	if err != nil {
		return nil, err
	}

	f := Field{
		Name: name,
		Type: t,
	}

	for _, item := range m[1:] {
		switch key := item.Key.(string); key {
		case "default":
			switch f.Type.T {
			case DataTypeVarchar, DataTypeInteger, DataTypeBigint, DataTypeBool,
				DataTypeDouble, DataTypeText:
				f.Default = item.Value
			default:
				return nil, fmt.Errorf("%s: data type '%s' can not have 'default' attribute", f.Name, f.Type.T)
			}

		case "size":
			switch f.Type.T {
			case DataTypeVarchar:
			default:
				return nil, fmt.Errorf("%s: data type '%s' can not have 'size' attribute", f.Name, f.Type.T)
			}
			f.Size = item.Value.(int)
		case "comment":
			f.Comment = item.Value.(string)
		case "nullable":
			f.Nullable = item.Value.(bool)
		case "pk":
			switch f.Type.T {
			case DataTypeInteger, DataTypeBigint:
			default:
				return nil, fmt.Errorf("%s: primary key must be integer, bigint", f.Name)
			}
			f.PK = item.Value.(bool)
		default:
			return nil, fmt.Errorf("%s: invalid attribute: %s", f.Name, key)
		}
	}

	// some validation
	switch f.Type.T {
	case DataTypeVarchar:
		if f.Size == 0 {
			return nil, fmt.Errorf("%s should have size. if size is not a consideration, 'text' should be used", f.Name)
		}
	}

	if f.PK {
		if f.Nullable {
			return nil, fmt.Errorf("%s: primary key can not be nullable", f.Name)
		}
	}

	return &f, nil
}

func (p *parser) parseDatatype(t string) (*Type, error) {
	switch t {
	case "i32":
		return &Type{T: DataTypeInteger}, nil
	case "i64":
		return &Type{T: DataTypeBigint}, nil
	case "str":
		return &Type{T: DataTypeVarchar}, nil
	case "bool":
		return &Type{T: DataTypeBool}, nil
	case "datetime":
		return &Type{T: DataTypeDatetime}, nil
	case "double":
		return &Type{T: DataTypeDouble}, nil
	case "text":
		return &Type{T: DataTypeText}, nil
	default:
		return nil, fmt.Errorf("invalid data type: %s", t)
	}
}

type Table struct {
	DB      string
	Name    string
	Comment string
	Fields  []*Field
	Indexes []Index
	Uniques []Index
}

type Index []string

type Field struct {
	Name     string
	Type     *Type
	Comment  string
	Nullable bool
	Default  interface{}
	Size     int // only 'varchar' has size attribute
	PK       bool
}

type Type struct {
	T string
}

func main() {
	flag.Parse()

	if *model == "" {
		return
	}

	exitIfErr(checkFile(*model))

	raw, err := readFile(*model)
	exitIfErr(err)

	// parse to get structured data
	p := parser{}

	exitIfErr(p.parse(raw))

	exitIfErr((&gen{}).render(&p, os.Stdout))
}

type gen struct {
	buf bytes.Buffer
}

func (g *gen) Pf(format string, args ...interface{}) *gen {
	g.buf.WriteString(fmt.Sprintf(format, args...))
	return g
}

func (g *gen) P(s ...string) *gen {
	g.buf.WriteString(strings.Join(s, " "))
	return g
}

func (g *gen) Ln() *gen {
	return g.P("\n")
}

func (g *gen) String() string {
	return g.buf.String()
}

func (g *gen) Write(w io.Writer) error {
	_, err := w.Write(g.buf.Bytes())
	return err
}

func (g *gen) render(p *parser, w io.Writer) error {
	g.P("-- Auto generated by xq, DO NOT MODIFY.").Ln()

	g.Ln().P("-- Tables").Ln().Ln()

	for i, t := range p.Tables {
		if len(t.Fields) == 0 {
			continue
		}

		if i > 0 {
			g.Ln()
		}

		// DDL
		g.P("create table if not exists", t.Name, "(").Ln()

		for j, f := range t.Fields {
			g.P(" ", f.Name, f.Type.T)

			// size
			if f.Size > 0 {
				switch f.Type.T {
				case DataTypeVarchar:
					g.Pf("(%d)", f.Size)
				}
			}

			// default
			if f.Default != nil {
				switch f.Type.T {
				case DataTypeVarchar, DataTypeText:
					g.Pf(" default '%s'", f.Default.(string))
				case DataTypeInteger, DataTypeBigint:
					g.Pf(" default %d", f.Default.(int))
				case DataTypeBool:
					g.Pf(" default %t", f.Default.(bool))
				}
			}

			if !f.Nullable && !f.PK {
				g.P(" not null")
			}

			if f.PK {
				g.P(" primary key")
			}

			if len(f.Comment) > 0 {
				g.Pf(" comment '%s'", f.Comment)
			}

			if j < len(t.Fields)-1 {
				g.P(",")
			}
			g.Ln()
		}

		g.P(")")

		if len(t.Comment) > 0 {
			g.Pf(" comment '%s'", t.Comment)
		}

		g.P(";").Ln()

		// indexes
		for _, index := range t.Uniques {
			g.Pf("create unique index uni_%s_%s on %s (%s);", t.Name, strings.Join(index, "_"), t.Name, strings.Join(index, ", ")).Ln()
		}

		for _, index := range t.Indexes {
			g.Pf("create index idx_%s_%s on %s (%s);", t.Name, strings.Join(index, "_"), t.Name, strings.Join(index, ", ")).Ln()
		}
	}

	return g.Write(w)
}

func checkFile(f string) error {
	if stat, err := os.Stat(f); os.IsNotExist(err) {
		return errors.New("error: model not found")
	} else if err != nil {
		return fmt.Errorf("unknown error: %s", err.Error())
	} else if stat.IsDir() {
		return errors.New("error: model should be a YAML file")
	}
	return nil
}

func readFile(path string) (yaml.MapSlice, error) {
	var raw yaml.MapSlice

	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(b, &raw); err != nil {
		return nil, err
	}

	return raw, nil
}

func exitIfErr(e error) {
	if e == nil {
		return
	}
	println(e.Error())
	os.Exit(1)
}
