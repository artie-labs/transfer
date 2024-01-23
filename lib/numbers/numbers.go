package numbers

type BetweenEqArgs struct {
	Start  int
	End    int
	Number int
}

// BetweenEq - Looks something like this. start <= number <= end
func BetweenEq(args BetweenEqArgs) bool {
	return args.Number >= args.Start && args.Number <= args.End
}
