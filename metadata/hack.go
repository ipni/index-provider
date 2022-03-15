package metadata

import (
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/multiformats/go-varint"
)

// ToStiMetadata is a hack to make code compile until this is merged:
// https://github.com/filecoin-project/storetheindex/pull/250
func (m *Metadata) ToStiMetadata() (v0.Metadata, error) {
	//TODO: pass in marshalled bytes once this is merged:
	//        https://github.com/filecoin-project/storetheindex/pull/250
	// For now, pick the first transport from metadata

	tp := m.protocols[0]
	binary, err := tp.MarshalBinary()
	if err != nil {
		return v0.Metadata{}, err
	}
	return v0.Metadata{
		ProtocolID: tp.ID(),
		Data:       binary[varint.UvarintSize(uint64(tp.ID())):],
	}, nil
}
