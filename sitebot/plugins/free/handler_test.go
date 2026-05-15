package free

import "testing"

func TestAggregateNamedGroupSumsMatchingMountPaths(t *testing.T) {
	statuses := []diskStatus{
		{
			Name: "LOCAL",
			Roots: []diskRootStatus{
				{Path: "/glftpd/site", MountPath: "/", Free: 10, Total: 100},
				{Path: "/glftpd/DISK1", MountPath: "/ARCHiVE", Free: 50, Total: 200},
				{Path: "/glftpd/DISK2", MountPath: "/ARCHiVE", Free: 70, Total: 300},
				{Path: "/glftpd/DISK8", MountPath: "/OFFSITE", Free: 90, Total: 400},
			},
		},
	}

	freeBytes, totalBytes, ok := aggregateNamedGroup(statuses, namedGroup{
		Name:       "ARCHiVE",
		MountPaths: []string{"/ARCHiVE"},
	})
	if !ok {
		t.Fatalf("expected group to match")
	}
	if freeBytes != 120 || totalBytes != 500 {
		t.Fatalf("aggregateNamedGroup() = %d/%d, want 120/500", freeBytes, totalBytes)
	}
}

func TestAggregateNamedGroupSumsMatchingPaths(t *testing.T) {
	statuses := []diskStatus{
		{
			Name: "LOCAL",
			Roots: []diskRootStatus{
				{Path: "/glftpd/DISK1", MountPath: "/ARCHiVE", Free: 50, Total: 200},
				{Path: "/glftpd/DISK2", MountPath: "/ARCHiVE", Free: 70, Total: 300},
				{Path: "/glftpd/DISK8", MountPath: "/OFFSITE", Free: 90, Total: 400},
			},
		},
	}

	freeBytes, totalBytes, ok := aggregateNamedGroup(statuses, namedGroup{
		Name:  "LOCAL ARCH DISKS",
		Paths: []string{"/glftpd/DISK1", "/glftpd/DISK2"},
	})
	if !ok {
		t.Fatalf("expected group to match")
	}
	if freeBytes != 120 || totalBytes != 500 {
		t.Fatalf("aggregateNamedGroup() = %d/%d, want 120/500", freeBytes, totalBytes)
	}
}
