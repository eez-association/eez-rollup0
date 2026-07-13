package plugin

import (
	"github.com/ethpandaops/spamoor/plugins/eez-rollup/xchain"
	"github.com/ethpandaops/spamoor/scenario"
)

// PluginDescriptor exposes the EEZ scenarios to Spamoor.
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
