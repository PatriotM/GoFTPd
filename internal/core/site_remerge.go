package core

import (
	"fmt"
	"path"
	"strings"
)

func (s *Session) HandleSiteRemerge(args []string) bool {
	if len(args) < 1 || len(args) > 2 {
		fmt.Fprintf(s.Conn, "501 Usage: SITE REMERGE <slave|*> [SITE|<path>]\r\n")
		return false
	}
	if s.Config.Mode != "master" || s.MasterManager == nil {
		fmt.Fprintf(s.Conn, "550 SITE REMERGE is only available in master mode.\r\n")
		return false
	}
	bridge, ok := s.MasterManager.(MasterBridge)
	if !ok {
		fmt.Fprintf(s.Conn, "550 Master not initialized.\r\n")
		return false
	}

	target := strings.TrimSpace(args[0])
	basePath := "/"
	rootsOnly := false
	scoped := false
	if len(args) == 2 {
		scoped = true
		scopeArg := strings.TrimSpace(args[1])
		if strings.EqualFold(scopeArg, "SITE") {
			basePath = "/"
			rootsOnly = true
		} else {
			basePath = path.Clean("/" + scopeArg)
		}
	}
	if target == "*" {
		var started int
		var errs []string
		if scoped {
			started, errs = bridge.StartRemergeAllPath(basePath, rootsOnly)
		} else {
			started, errs = bridge.StartRemergeAll()
		}
		if started == 0 && len(errs) > 0 {
			fmt.Fprintf(s.Conn, "550 REMERGE failed: %s\r\n", strings.Join(errs, "; "))
			return false
		}
		if len(errs) > 0 {
			fmt.Fprintf(s.Conn, "200 REMERGE started for %d slave(s); skipped: %s\r\n", started, strings.Join(errs, "; "))
			return false
		}
		fmt.Fprintf(s.Conn, "200 REMERGE started for %d slave(s).\r\n", started)
		return false
	}

	var err error
	if scoped {
		err = bridge.StartRemergePath(target, basePath, rootsOnly)
	} else {
		err = bridge.StartRemerge(target)
	}
	if err != nil {
		fmt.Fprintf(s.Conn, "550 REMERGE failed: %v\r\n", err)
		return false
	}
	if scoped {
		if rootsOnly {
			fmt.Fprintf(s.Conn, "200 REMERGE started for %s (site roots only).\r\n", target)
			return false
		}
		fmt.Fprintf(s.Conn, "200 REMERGE started for %s at %s.\r\n", target, basePath)
		return false
	}
	fmt.Fprintf(s.Conn, "200 REMERGE started for %s.\r\n", target)
	return false
}

func (s *Session) HandleSiteRemergeStop(args []string) bool {
	if len(args) != 1 {
		fmt.Fprintf(s.Conn, "501 Usage: SITE REMERGESTOP <slave|*>\r\n")
		return false
	}
	if s.Config.Mode != "master" || s.MasterManager == nil {
		fmt.Fprintf(s.Conn, "550 SITE REMERGESTOP is only available in master mode.\r\n")
		return false
	}
	bridge, ok := s.MasterManager.(MasterBridge)
	if !ok {
		fmt.Fprintf(s.Conn, "550 Master not initialized.\r\n")
		return false
	}
	target := strings.TrimSpace(args[0])
	if target == "*" {
		stopped, errs := bridge.StopRemergeAll()
		if stopped == 0 && len(errs) > 0 {
			fmt.Fprintf(s.Conn, "550 REMERGESTOP failed: %s\r\n", strings.Join(errs, "; "))
			return false
		}
		if len(errs) > 0 {
			fmt.Fprintf(s.Conn, "200 REMERGESTOP sent to %d slave(s); skipped: %s\r\n", stopped, strings.Join(errs, "; "))
			return false
		}
		fmt.Fprintf(s.Conn, "200 REMERGESTOP sent to %d slave(s).\r\n", stopped)
		return false
	}
	if err := bridge.StopRemerge(target); err != nil {
		fmt.Fprintf(s.Conn, "550 REMERGESTOP failed: %v\r\n", err)
		return false
	}
	fmt.Fprintf(s.Conn, "200 REMERGESTOP sent to %s.\r\n", target)
	return false
}
