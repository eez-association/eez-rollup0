# Local Stateless extension

Based on eez-association/stateless commit 4fc3806bdd0e6b296c761ef4d4b260938365cf45.
The recovered-block validation entry points accept generic transaction and receipt
primitives. Ethereum defaults and the public-key recovery API remain unchanged.
Witness verification, consensus checks, checkpoint derivation, and trie algorithms
are unchanged. The tries dependency is still pinned to that upstream commit.

This local dependency makes the EEZ native-type implementation reproducible without
requiring an unpublished remote branch. Replace it with a pinned upstream revision
once the generic API is available there.
