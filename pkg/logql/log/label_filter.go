package log

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/prometheus/prometheus/model/labels"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/grafana/loki/pkg/logqlmodel"
)

var (
	_ LabelFilterer = &BinaryLabelFilter{}
	_ LabelFilterer = &BytesLabelFilter{}
	_ LabelFilterer = &DurationLabelFilter{}
	_ LabelFilterer = &NumericLabelFilter{}
	_ LabelFilterer = &StringLabelFilter{}
)

// LabelFilterType is an enum for label filtering types.
type LabelFilterType int

// Possible LabelFilterType.
const (
	LabelFilterEqual LabelFilterType = iota
	LabelFilterNotEqual
	LabelFilterGreaterThan
	LabelFilterGreaterThanOrEqual
	LabelFilterLesserThan
	LabelFilterLesserThanOrEqual
)

func (f LabelFilterType) String() string {
	switch f {
	case LabelFilterEqual:
		return "=="
	case LabelFilterNotEqual:
		return "!="
	case LabelFilterGreaterThan:
		return ">"
	case LabelFilterGreaterThanOrEqual:
		return ">="
	case LabelFilterLesserThan:
		return "<"
	case LabelFilterLesserThanOrEqual:
		return "<="
	default:
		return ""
	}
}

// LabelFilterer can filter extracted labels.
type LabelFilterer interface {
	Stage
	fmt.Stringer
}

type BinaryLabelFilter struct {
	Left  LabelFilterer
	Right LabelFilterer
	and   bool
}

// NewAndLabelFilter creates a new LabelFilterer from a and binary operation of two LabelFilterer.
func NewAndLabelFilter(left LabelFilterer, right LabelFilterer) *BinaryLabelFilter {
	return &BinaryLabelFilter{
		Left:  left,
		Right: right,
		and:   true,
	}
}

// NewOrLabelFilter creates a new LabelFilterer from a or binary operation of two LabelFilterer.
func NewOrLabelFilter(left LabelFilterer, right LabelFilterer) *BinaryLabelFilter {
	return &BinaryLabelFilter{
		Left:  left,
		Right: right,
	}
}

func (b *BinaryLabelFilter) Process(ts int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	line, lok := b.Left.Process(ts, line, lbs)
	if !b.and && lok {
		return line, true
	}
	line, rok := b.Right.Process(ts, line, lbs)
	if !b.and {
		return line, lok || rok
	}
	return line, lok && rok
}

func (b *BinaryLabelFilter) RequiredLabelNames() []string {
	var names []string
	names = append(names, b.Left.RequiredLabelNames()...)
	names = append(names, b.Right.RequiredLabelNames()...)
	return uniqueString(names)
}

func (b *BinaryLabelFilter) String() string {
	var sb strings.Builder
	sb.WriteString("( ")
	sb.WriteString(b.Left.String())
	if b.and {
		sb.WriteString(" , ")
	} else {
		sb.WriteString(" or ")
	}
	sb.WriteString(b.Right.String())
	sb.WriteString(" )")
	return sb.String()
}

type NoopLabelFilter struct {
	*labels.Matcher
}

func (NoopLabelFilter) Process(_ int64, line []byte, _ *LabelsBuilder) ([]byte, bool) {
	return line, true
}
func (NoopLabelFilter) RequiredLabelNames() []string { return []string{} }

func (f NoopLabelFilter) String() string {
	if f.Matcher != nil {
		return f.Matcher.String()
	}
	return ""
}

// ReduceAndLabelFilter Reduces multiple label filterer into one using binary and operation.
func ReduceAndLabelFilter(filters []LabelFilterer) LabelFilterer {
	if len(filters) == 0 {
		return &NoopLabelFilter{}
	}
	if len(filters) == 1 {
		return filters[0]
	}
	result := filters[0]
	for _, f := range filters[1:] {
		result = NewAndLabelFilter(result, f)
	}
	return result
}

type BytesLabelFilter struct {
	Name  string
	Value uint64
	Type  LabelFilterType
}

// NewBytesLabelFilter creates a new label filterer which parses bytes string representation (1KB) from the value of the named label
// and compares it with the given b value.
func NewBytesLabelFilter(t LabelFilterType, name string, b uint64) *BytesLabelFilter {
	return &BytesLabelFilter{
		Name:  name,
		Type:  t,
		Value: b,
	}
}

func (d *BytesLabelFilter) Process(_ int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	if lbs.HasErr() {
		// if there's an error only the string matchers can filter it out.
		return line, true
	}
	v, ok := lbs.Get(d.Name)
	if !ok {
		// we have not found this label.
		return line, false
	}
	value, err := humanize.ParseBytes(v)
	if err != nil {
		lbs.SetErr(errLabelFilter)
		lbs.SetErrorDetails(err.Error())
		return line, true
	}
	switch d.Type {
	case LabelFilterEqual:
		return line, value == d.Value
	case LabelFilterNotEqual:
		return line, value != d.Value
	case LabelFilterGreaterThan:
		return line, value > d.Value
	case LabelFilterGreaterThanOrEqual:
		return line, value >= d.Value
	case LabelFilterLesserThan:
		return line, value < d.Value
	case LabelFilterLesserThanOrEqual:
		return line, value <= d.Value
	default:
		lbs.SetErr(errLabelFilter)
		return line, true
	}
}

func (d *BytesLabelFilter) RequiredLabelNames() []string {
	return []string{d.Name}
}

func (d *BytesLabelFilter) String() string {
	b := strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, humanize.Bytes(d.Value))
	return fmt.Sprintf("%s%s%s", d.Name, d.Type, b)
}

type DurationLabelFilter struct {
	Name  string
	Value time.Duration
	Type  LabelFilterType
}

// NewDurationLabelFilter creates a new label filterer which parses duration string representation (5s)
// from the value of the named label and compares it with the given d value.
func NewDurationLabelFilter(t LabelFilterType, name string, d time.Duration) *DurationLabelFilter {
	return &DurationLabelFilter{
		Name:  name,
		Type:  t,
		Value: d,
	}
}

func (d *DurationLabelFilter) Process(_ int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	if lbs.HasErr() {
		// if there's an error only the string matchers can filter out.
		return line, true
	}
	v, ok := lbs.Get(d.Name)
	if !ok {
		// we have not found this label.
		return line, false
	}
	value, err := time.ParseDuration(v)
	if err != nil {
		lbs.SetErr(errLabelFilter)
		lbs.SetErrorDetails(err.Error())
		return line, true
	}
	switch d.Type {
	case LabelFilterEqual:
		return line, value == d.Value
	case LabelFilterNotEqual:
		return line, value != d.Value
	case LabelFilterGreaterThan:
		return line, value > d.Value
	case LabelFilterGreaterThanOrEqual:
		return line, value >= d.Value
	case LabelFilterLesserThan:
		return line, value < d.Value
	case LabelFilterLesserThanOrEqual:
		return line, value <= d.Value
	default:
		lbs.SetErr(errLabelFilter)
		return line, true
	}
}

func (d *DurationLabelFilter) RequiredLabelNames() []string {
	return []string{d.Name}
}

func (d *DurationLabelFilter) String() string {
	return fmt.Sprintf("%s%s%s", d.Name, d.Type, d.Value)
}

type NumericLabelFilter struct {
	Name  string
	Value float64
	Type  LabelFilterType
	err   error
}

// NewNumericLabelFilter creates a new label filterer which parses float64 string representation (5.2)
// from the value of the named label and compares it with the given f value.
func NewNumericLabelFilter(t LabelFilterType, name string, v float64) *NumericLabelFilter {
	return &NumericLabelFilter{
		Name:  name,
		Type:  t,
		Value: v,
	}
}

func (n *NumericLabelFilter) Process(_ int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	if lbs.HasErr() {
		// if there's an error only the string matchers can filter out.
		return line, true
	}
	v, ok := lbs.Get(n.Name)
	if !ok {
		// we have not found this label.
		return line, false
	}
	value, err := strconv.ParseFloat(v, 64)
	if err != nil {
		lbs.SetErr(errLabelFilter)
		lbs.SetErrorDetails(err.Error())
		return line, true
	}
	switch n.Type {
	case LabelFilterEqual:
		return line, value == n.Value
	case LabelFilterNotEqual:
		return line, value != n.Value
	case LabelFilterGreaterThan:
		return line, value > n.Value
	case LabelFilterGreaterThanOrEqual:
		return line, value >= n.Value
	case LabelFilterLesserThan:
		return line, value < n.Value
	case LabelFilterLesserThanOrEqual:
		return line, value <= n.Value
	default:
		lbs.SetErr(errLabelFilter)
		return line, true
	}

}

func (n *NumericLabelFilter) RequiredLabelNames() []string {
	return []string{n.Name}
}

func (n *NumericLabelFilter) String() string {
	return fmt.Sprintf("%s%s%s", n.Name, n.Type, strconv.FormatFloat(n.Value, 'f', -1, 64))
}

type StringLabelFilter struct {
	*labels.Matcher
}

// NewStringLabelFilter creates a new label filterer which compares string label.
// This is the only LabelFilterer that can filter out the __error__ label.
// Unlike other LabelFilterer which apply conversion, if the label name doesn't exist it is compared with an empty value.
func NewStringLabelFilter(m *labels.Matcher) LabelFilterer {
	f, err := NewLabelFilter(m.Value, m.Type)
	if err != nil {
		return &StringLabelFilter{Matcher: m}
	}

	if f == TrueFilter {
		return &NoopLabelFilter{m}
	}

	return &lineFilterLabelFilter{
		Matcher: m,
		filter:  f,
	}
}

func (s *StringLabelFilter) Process(_ int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	return line, s.Matches(labelValue(s.Name, lbs))
}

func (s *StringLabelFilter) RequiredLabelNames() []string {
	return []string{s.Name}
}

// lineFilterLabelFilter filters the desired label using an optimized line filter
type lineFilterLabelFilter struct {
	*labels.Matcher
	filter Filterer
}

func (s *lineFilterLabelFilter) Process(_ int64, line []byte, lbs *LabelsBuilder) ([]byte, bool) {
	v := labelValue(s.Name, lbs)
	return line, s.filter.Filter(unsafeGetBytes(v))
}

func (s *lineFilterLabelFilter) RequiredLabelNames() []string {
	return []string{s.Name}
}

func labelValue(name string, lbs *LabelsBuilder) string {
	if name == logqlmodel.ErrorLabel {
		return lbs.GetErr()
	}
	v, _ := lbs.Get(name)
	return v
}
