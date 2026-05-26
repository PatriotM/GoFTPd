package core

import (
	"fmt"
	"strings"
)

func (s *Session) HandleSiteRemerge(args []string) bool {
	req, err := parseSiteRemergeArgs(args)
	if err != nil {
		fmt.Fprintf(s.Conn, "501 Usage: SITE REMERGE <slave|*> [job] [path] (%v)\r\n", err)
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

	if req.target == "*" {
		var started int
		var errs []string
		if req.jobName != "" {
			started, errs = bridge.StartRemergeAllJob(req.jobName, req.path)
		} else {
			started, errs = bridge.StartRemergeAllJobs()
		}
		if started == 0 && len(errs) > 0 {
			fmt.Fprintf(s.Conn, "550 REMERGE failed: %s\r\n", strings.Join(errs, "; "))
			return false
		}
		if len(errs) > 0 {
			fmt.Fprintf(s.Conn, "200 REMERGE requested %d configured scan(s); skipped: %s\r\n", started, strings.Join(errs, "; "))
			return false
		}
		fmt.Fprintf(s.Conn, "200 REMERGE requested %d configured scan(s).\r\n", started)
		return false
	}

	var started int
	var errs []string
	if req.jobName != "" {
		started, errs = bridge.StartRemergeJob(req.target, req.jobName, req.path)
	} else {
		started, errs = bridge.StartRemergeJobs(req.target)
	}
	if started == 0 && len(errs) > 0 {
		fmt.Fprintf(s.Conn, "550 REMERGE failed: %s\r\n", strings.Join(errs, "; "))
		return false
	}
	if len(errs) > 0 {
		fmt.Fprintf(s.Conn, "200 REMERGE requested for %s (%d configured scan(s)); skipped: %s\r\n", req.target, started, strings.Join(errs, "; "))
		return false
	}
	fmt.Fprintf(s.Conn, "200 REMERGE requested for %s (%d configured scan(s)).\r\n", req.target, started)
	return false
}

type siteRemergeRequest struct {
	target  string
	jobName string
	path    string
}

func parseSiteRemergeArgs(args []string) (siteRemergeRequest, error) {
	if len(args) < 1 || len(args) > 3 {
		return siteRemergeRequest{}, fmt.Errorf("invalid argument count")
	}
	target := strings.TrimSpace(args[0])
	if target == "" {
		return siteRemergeRequest{}, fmt.Errorf("missing target")
	}
	req := siteRemergeRequest{target: target}
	if len(args) >= 2 {
		req.jobName = strings.TrimSpace(args[1])
		if req.jobName == "" {
			return siteRemergeRequest{}, fmt.Errorf("missing job")
		}
	}
	if len(args) == 3 {
		req.path = strings.TrimSpace(args[2])
		if req.path == "" {
			return siteRemergeRequest{}, fmt.Errorf("missing path")
		}
		if !strings.HasPrefix(req.path, "/") {
			return siteRemergeRequest{}, fmt.Errorf("path must be an absolute VFS path")
		}
	}
	return req, nil
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
