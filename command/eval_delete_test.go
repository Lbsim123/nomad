package command

import (
	"errors"
	"testing"

	"github.com/hashicorp/nomad/ci"
	"github.com/mitchellh/cli"
	"github.com/stretchr/testify/require"
)

func TestEvalDeleteCommand_Run(t *testing.T) {
	ci.Parallel(t)

	testCases := []struct {
		testFn func()
		name   string
	}{
		{
			testFn: func() {

				testServer, client, url := testServer(t, false, nil)
				defer testServer.Shutdown()

				// Create the UI and command.
				ui := cli.NewMockUi()
				cmd := &EvalDeleteCommand{
					Meta: Meta{
						Ui:          ui,
						flagAddress: url,
					},
				}

				// Test basic command input validation.
				require.Equal(t, 1, cmd.Run([]string{"-address=" + url}))
				require.Contains(t, ui.ErrorWriter.String(), "Error validating command args and flags")
				ui.ErrorWriter.Reset()
				ui.OutputWriter.Reset()

				// Try deleting an eval by its ID that doesn't exist.
				require.Equal(t, 1, cmd.Run([]string{"-address=" + url, "fa3a8c37-eac3-00c7-3410-5ba3f7318fd8"}))
				require.Contains(t, ui.ErrorWriter.String(), "404 (eval not found)")
				ui.ErrorWriter.Reset()
				ui.OutputWriter.Reset()

				// Ensure the scheduler config broker is un-paused.
				schedulerConfig, _, err := client.Operator().SchedulerGetConfiguration(nil)
				require.NoError(t, err)
				require.False(t, schedulerConfig.SchedulerConfig.PauseEvalBroker)
			},
			name: "failures",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.testFn()
		})
	}
}

func TestEvalDeleteCommand_verifyArgsAndFlags(t *testing.T) {
	ci.Parallel(t)

	testCases := []struct {
		inputEvalDeleteCommand *EvalDeleteCommand
		inputArgs              []string
		expectedError          error
		name                   string
	}{
		{
			inputEvalDeleteCommand: &EvalDeleteCommand{
				filter: `Status == "Pending"`,
			},
			inputArgs:     []string{"fa3a8c37-eac3-00c7-3410-5ba3f7318fd8"},
			expectedError: errors.New("evaluation ID or filter flag required"),
			name:          "arg and flags",
		},
		{
			inputEvalDeleteCommand: &EvalDeleteCommand{
				filter: "",
			},
			inputArgs:     []string{},
			expectedError: errors.New("evaluation ID or filter flag required"),
			name:          "no arg or flags",
		},
		{
			inputEvalDeleteCommand: &EvalDeleteCommand{
				filter: "",
			},
			inputArgs:     []string{"fa3a8c37-eac3-00c7-3410-5ba3f7318fd8", "fa3a8c37-eac3-00c7-3410-5ba3f7318fd9"},
			expectedError: errors.New("expected 1 argument, got 2"),
			name:          "multiple args",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualError := tc.inputEvalDeleteCommand.verifyArgsAndFlags(tc.inputArgs)
			require.Equal(t, tc.expectedError, actualError)
		})
	}
}
