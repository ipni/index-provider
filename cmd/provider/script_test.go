package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rogpeppe/go-internal/testscript"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"provider": func() { run() },
	})
}

func TestScript(t *testing.T) {
	t.Skip("Unstable in CI")
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
