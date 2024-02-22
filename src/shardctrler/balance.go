package shardctrler

import (
	"fmt"
	"log"
	"sort"
)

type GroupItem struct {
	gid    int
	shards []int
}

func (gi GroupItem) String() string {
	return fmt.Sprintf("{gid=%v, shards=%v}", gi.gid, gi.shards)
}

type GroupItemList []GroupItem

func (gil GroupItemList) Len() int {
	return len(gil)
}

func (gil GroupItemList) Less(i, j int) bool {
	gili := gil[i]
	gilj := gil[j]
	if len(gili.shards) > len(gilj.shards) {
		return true
	} else if len(gili.shards) == len(gilj.shards) {
		// for deterministic
		return gili.gid > gilj.gid
	} else {
		return false
	}
}

func (gil GroupItemList) Swap(i, j int) {
	gil[j], gil[i] = gil[i], gil[j]
}

func reBalance(items GroupItemList) (balancedItems GroupItemList) {
	if len(items) == 0 {
		return items
	}
	// for deterministic, sort first
	balancedItems = make([]GroupItem, items.Len())
	{
		copy(balancedItems, items)
		sort.Sort(balancedItems)
	}

	// calculate expect count
	expected := make([]int, len(balancedItems))
	{
		average := NShards / len(items)
		left := NShards - len(items)*average
		for k := 0; k < left; k++ {
			expected[k] = average + 1
		}
		for k := left; k < len(balancedItems); k++ {
			expected[k] = average
		}
	}

	// collect not assign
	var notAssignShardList []int
	{
		notAssignShardMap := make(map[int]bool)
		for shard := 0; shard < NShards; shard++ {
			notAssignShardMap[shard] = true
		}
		for _, item := range balancedItems {
			for _, shard := range item.shards {
				delete(notAssignShardMap, shard)
			}
		}

		for shard, _ := range notAssignShardMap {
			notAssignShardList = append(notAssignShardList, shard)
		}
		sort.Sort(sort.IntSlice(notAssignShardList))
	}

	// remove the number of occurrence > expect
	for k := range balancedItems {
		item := &balancedItems[k]
		if len(item.shards) > expected[k] {
			notAssignShardList = append(notAssignShardList, item.shards[expected[k]:]...)
			item.shards = item.shards[:expected[k]]
		}
	}

	// put if the number of occurrence < expect
	for k := range balancedItems {
		item := &balancedItems[k]
		lackN := expected[k] - len(item.shards)
		if lackN > 0 {
			item.shards = append(item.shards, notAssignShardList[:lackN]...)
			notAssignShardList = notAssignShardList[lackN:]
		}
	}

	if len(notAssignShardList) != 0 {
		log.Panicf("notAssignShardList (%v) is not empty.  items=%v, balancedItems=%v, expected=%v.", notAssignShardList, items, balancedItems, expected)
	}
	return
}

// gidList use to filter results. If gidList is not nil, then gid not in gidList will be filtered.
func shardToGroupItemList(shards [NShards]int, gidList []int) GroupItemList {
	gidToShards := make(map[int][]int)
	for _, gid := range gidList {
		gidToShards[gid] = []int{}
	}
	for shard, gid := range shards {
		present := true
		if gidList != nil {
			_, present = gidToShards[gid]
		}
		if present {
			gidToShards[gid] = append(gidToShards[gid], shard)
		}
	}
	var gil GroupItemList
	for gid, gShards := range gidToShards {
		if gid != 0 {
			gil = append(gil, GroupItem{gid, gShards})
		}

	}
	return gil
}

func groupItemListToShard(gil GroupItemList) (shards [NShards]int, groupN int) {
	groupN = gil.Len()
	for _, item := range gil {
		for _, shard := range item.shards {
			shards[shard] = item.gid
		}
	}
	return shards, groupN
}
