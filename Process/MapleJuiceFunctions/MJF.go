package MJF

import (
	"strconv"
	"strings"
)

func CondorcetMapper1(input []string) [][][]string {
	mapledData := [][][]string{}

	m := len(input)
	for i := 0; i < m-1; i++ {
		for j := i + 1; j < m; j++ {
			if input[i] == "" || input[j] == "" {
				continue
			}
			if input[i] < input[j] {
				temp := [][]string{[]string{input[i], input[j]}, []string{"1"}}
				mapledData = append(mapledData, temp)
			} else {
				temp := [][]string{[]string{input[j], input[i]}, []string{"0"}}
				mapledData = append(mapledData, temp)
			}
		}
	}
	return mapledData
}

func CondorcetReducer1(input [][]string) []string {
	Acount := 0
	Bcount := 0

	var keyA string
	var keyB string
	for _, line := range input {
		keys := strings.Split(line[0], ":")
		entry := line[1]

		keyA = keys[0]
		keyB = keys[1]

		if entry == "1" { // A won
			Acount++
		} else { // B won
			Bcount++
		}
	}

	if Acount > Bcount {
		return []string{keyA, keyB}
	}
	return []string{keyB, keyA}
}

//identity
func CondorcetMapper2(input []string) [][][]string {
	mapledData := [][][]string{}

	var key []string
	var value []string

	key = append(key,"1")
	value = append(value, input[0])
	value = append(value,input[1])
	candidates := [][]string{key, value}

	mapledData = append(mapledData, candidates)
	return mapledData
}

func CondorcetReducer2(input [][]string) []string {

	Carray := []int{0, 0, 0}

	for _, line := range input {
		values := strings.Split(line[1], ",")
		winner, _ := strconv.Atoi(values[0])
		winner_idx := winner - 1

		Carray[winner_idx]++
	}

	_, max := MinMax(Carray)

	winners := []string{}

	for idx, score := range Carray {
		if score == max {
			candidate := strconv.Itoa(idx + 1)
			winners = append(winners, candidate)
		}
	}

	if len(winners) == 1 {
		return []string{winners[0], "Condorcet Winner!"}
	} else {
		set := ""

		for _, winner := range winners {
			set = set + winner + " "
		}
		return []string{set, "No Condorcet winner, Highest Condorcet counts"}
	}
}

func MinMax(array []int) (int, int) {
	var max int = array[0]
	var min int = array[0]
	for _, value := range array {
		if max < value {
			max = value
		}
		if min > value {
			min = value
		}
	}
	return min, max
}
