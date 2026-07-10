package plugin

import (
	"github.com/ethpandaops/spamoor/plugins/eez-rollup/xchain"
	"github.com/ethpandaops/spamoor/scenario"
)

// PluginDescriptor defines the plugin metadata and scenarios. Loaded by
// spamoor's Yaegi interpreter at runtime — see infra/kurtosis/README.md for
// how this is wired into the devnet.
var PluginDescriptor = scenario.PluginDescriptor{
	Name:        "eez-rollup",
	Description: "Continuous L1<->L2 cross-chain load for the EEZ devnet (inbound + outbound fronts)",
	Categories: []*scenario.Category{
		{
			Name:        "EEZ",
			Description: "EEZ cross-chain scenarios",
			Descriptors: []*scenario.Descriptor{
				&xchain.ScenarioDescriptor,
			},
		},
	},
}
