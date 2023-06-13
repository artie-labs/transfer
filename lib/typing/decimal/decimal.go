package decimal

import "math/big"

type Decimal struct {
	scale     int
	precision int
	value     *big.Float
}

func NewDecimal(scale, precision int, value *big.Float) *Decimal {
	return &Decimal{
		scale:     scale,
		precision: precision,
		value:     value,
	}
}

func (d *Decimal) Scale() int {
	return d.scale
}

func (d *Decimal) Precision() int {
	return d.precision
}

func (d *Decimal) Value() interface{} {
	// Depending on the precision, we will want to convert value to STRING or keep as a FLOAT.
	return nil
}
