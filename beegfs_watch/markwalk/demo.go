package main

// The -demo mode. No network, no Watch: it builds small trees in a temp directory and runs
// the same walk over them, so you can see the five situations that make the walk necessary.

import (
	"fmt"
	"os"
	"path/filepath"
)

func runDemo() {
	tmp, err := os.MkdirTemp("", "markwalk")
	if err != nil {
		fmt.Println("mktemp:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	for _, d := range demos {
		mount := filepath.Join(tmp, d.name, "mount")
		index := filepath.Join(tmp, d.name, "index")
		d.setup(mount, index)

		fmt.Printf("\n%s\n  event names: %s\n  marks:", d.title, d.mark)
		for _, c := range markChain(filepath.Join(mount, d.mark), mount, index, newStatCache(64).stat) {
			r, _ := filepath.Rel(mount, c)
			fmt.Printf(" %s", r)
		}
		fmt.Println()
	}
}

var demos = []struct {
	name, title, mark string
	setup             func(mount, index string)
}{
	{"plain", "1. the normal case: the parent is already indexed", "project/data",
		func(m, i string) { mkTree(m, "project/data"); mkIndexed(i, "", "project") }},

	{"deleted", "2. the directory was deleted before we ran", "project/gone",
		func(m, i string) { mkTree(m, "project"); mkIndexed(i, "", "project") }},

	{"gap", "3. a silent client created d1/d2/d3 and none of it is indexed", "d1/d2/d3",
		func(m, i string) { mkTree(m, "d1/d2/d3"); mkIndexed(i, "") }},

	{"halfbuilt", "4. an index directory exists but has no db.db", "big/beyond/deeper",
		func(m, i string) {
			mkTree(m, "big/beyond/deeper")
			mkIndexed(i, "", "big")
			os.MkdirAll(filepath.Join(i, "big/beyond"), 0o755) // what a failed run leaves
		}},

	{"replaced", "5. the directory is now a file", "project/data",
		func(m, i string) {
			mkTree(m, "project")
			mkIndexed(i, "", "project")
			os.WriteFile(filepath.Join(m, "project/data"), []byte("not a directory"), 0o644)
		}},
}

func mkTree(mount string, dirs ...string) {
	os.MkdirAll(mount, 0o755)
	for _, d := range dirs {
		os.MkdirAll(filepath.Join(mount, d), 0o755)
	}
}

// mkIndexed mirrors dirs into the index with a db.db each, the way a finished run leaves them.
func mkIndexed(index string, dirs ...string) {
	for _, d := range dirs {
		p := filepath.Join(index, d)
		os.MkdirAll(p, 0o755)
		os.WriteFile(filepath.Join(p, "db.db"), []byte("pretend sqlite"), 0o644)
	}
}
