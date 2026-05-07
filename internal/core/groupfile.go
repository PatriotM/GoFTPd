package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type GroupFile struct {
	Name            string
	Slots           int
	LeechSlots      int
	AllotmentSlots  int
	MaxAllotment    int64
	GroupNFO        string
	Simult          int
}

func LoadGroupConfig(name string) (*GroupFile, error) {
	path := filepath.Join("etc", "groups", name)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	g := &GroupFile{
		Name:           name,
		Slots:          -1,
		LeechSlots:     0,
		AllotmentSlots: 0,
		MaxAllotment:   0,
		Simult:         0,
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch strings.ToUpper(parts[0]) {
		case "GROUP":
			if len(parts) > 1 {
				g.Name = parts[1]
			}
		case "SLOTS":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &g.Slots)
			}
			if len(parts) > 2 {
				fmt.Sscanf(parts[2], "%d", &g.LeechSlots)
			}
			if len(parts) > 3 {
				fmt.Sscanf(parts[3], "%d", &g.AllotmentSlots)
			}
			if len(parts) > 4 {
				fmt.Sscanf(parts[4], "%d", &g.MaxAllotment)
			}
		case "GROUPNFO":
			if len(parts) > 1 {
				g.GroupNFO = strings.Join(parts[1:], " ")
			}
		case "SIMULT":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &g.Simult)
			}
		}
	}
	return g, nil
}

func (g *GroupFile) Save() error {
	if g == nil {
		return fmt.Errorf("group config is nil")
	}
	if g.Name == "" {
		return fmt.Errorf("group name is empty")
	}
	path := filepath.Join("etc", "groups", g.Name)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintf(file, "GROUP %s\n", g.Name)
	fmt.Fprintf(file, "SLOTS %d %d %d %d\n", g.Slots, g.LeechSlots, g.AllotmentSlots, g.MaxAllotment)
	fmt.Fprintf(file, "GROUPNFO %s\n", strings.TrimSpace(g.GroupNFO))
	fmt.Fprintf(file, "SIMULT %d\n", g.Simult)
	return nil
}
