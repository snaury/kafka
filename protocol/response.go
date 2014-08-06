package protocol

type ResponseHeader struct {
	CorrelationId int32
}

func (h *ResponseHeader) Decode(pd PacketDecoder) (err error) {
	h.CorrelationId, err = pd.GetInt32()
	if err != nil {
		return
	}
	return nil
}

func (h *ResponseHeader) Encode(pe PacketEncoder) (err error) {
	pe.PutInt32(h.CorrelationId)
	return nil
}
