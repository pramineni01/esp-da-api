package esputils

import (
	"fmt"
)

func ErrDBNoRows(op string) error {
	return fmt.Errorf("No matching rows found for operation: %s", op)
}
