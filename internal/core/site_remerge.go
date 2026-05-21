package core

import (
	"fmt"
	"strings"
)

func (s *Session) HandleSiteRemerge(args []string) bool {
	target, err := parseSiteRemergeArgs(args)
	if err != nil {
		fmt.Fprintf(s.Conn, "501 Usage: SITE REMERGE <slave|*> (%v)\r\n", err)
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

	if target == "*" {
		started, errs := bridge.StartRemergeAllJobs()
		if started == 0 && len(errs) > 0 {
			fmt.Fprintf(s.Conn, "550 REMERGE failed: %s\r\n", strings.Join(errs, "; "))
			return false
		}
		if len(errs) > 0 {
			fmt.Fprintf(s.Conn, "200 REMERGE started %d configured scan(s); skipped: %s\r\n", started, strings.Join(errs, "; "))
			return false
		}
		fmt.Fprintf(s.Conn, "200 REMERGE started %d configured scan(s).\r\n", started)
		return false
	}

	started, errs := bridge.StartRemergeJobs(target)
	if started == 0 && len(errs) > 0 {
		fmt.Fprintf(s.Conn, "550 REMERGE failed: %s\r\n", strings.Join(errs, "; "))
		return false
	}
	if len(errs) > 0 {
		fmt.Fprintf(s.Conn, "200 REMERGE started for %s (%d configured scan(s)); skipped: %s\r\n", target, started, strings.Join(errs, "; "))
		return false
	}
	fmt.Fprintf(s.Conn, "200 REMERGE started for %s (%d configured scan(s)).\r\n", target, started)
	return false
}

func parseSiteRemergeArgs(args []string) (target string, err error) {
	if len(args) != 1 {
		return "", fmt.Errorf("invalid argument count")
	}
	target = strings.TrimSpace(args[0])
	if target == "" {
		return "", fmt.Errorf("missing target")
	}
	return target, nil
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
