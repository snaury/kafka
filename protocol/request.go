package protocol

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

func (h *RequestHeader) Decode(pd PacketDecoder) (err error) {
	h.ApiKey, err = pd.GetInt16()
	if err != nil {
		return
	}
	h.ApiVersion, err = pd.GetInt16()
	if err != nil {
		return
	}
	h.CorrelationId, err = pd.GetInt32()
	if err != nil {
		return
	}
	h.ClientId, err = pd.GetString()
	if err != nil {
		return
	}
	return nil
}

func (h *RequestHeader) Encode(pe PacketEncoder) (err error) {
	pe.PutInt16(h.ApiKey)
	pe.PutInt16(h.ApiVersion)
	pe.PutInt32(h.CorrelationId)
	err = pe.PutString(h.ClientId)
	return err
}
