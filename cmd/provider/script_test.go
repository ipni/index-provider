package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p-testing/ci"
	"github.com/rogpeppe/go-internal/testscript"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, map[string]func() int{
		"provider": run,
	}))
}

func TestScript(t *testing.T) {
	if !ci.IsRunning() {
		t.Skip("Skipping when run in CI")
	}
	t.Parallel()
	testscript.Run(t, testscript.Params{
		Dir: filepath.Join("testdata", "script"),
		Setup: func(env *testscript.Env) error {
			wd, err := os.Getwd()
			require.NoError(t, err)
			env.Setenv("TESTDATA", filepath.Join(wd, "..", "..", "testdata"))
			env.Setenv("GOLOG_LOG_LEVEL", "error")
			return nil
		},
	})
}
