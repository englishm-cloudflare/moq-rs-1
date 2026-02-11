use crate::coding::{Decode, DecodeError, Encode, EncodeError, ReasonPhrase};

#[derive(Clone, Debug)]
pub struct RequestError {
    pub id: u64,

    // An error code.
    pub error_code: u64,

    // Minimum time (ms) before the request should be retried, plus one.
    // 0 means the request should not be retried.
    pub retry_interval: u64,

    // An optional, human-readable reason.
    pub reason_phrase: ReasonPhrase,
}

impl Decode for RequestError {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let error_code = u64::decode(r)?;
        let retry_interval = u64::decode(r)?;
        let reason_phrase = ReasonPhrase::decode(r)?;

        Ok(Self {
            id,
            error_code,
            retry_interval,
            reason_phrase,
        })
    }
}

impl Encode for RequestError {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.error_code.encode(w)?;
        self.retry_interval.encode(w)?;
        self.reason_phrase.encode(w)?;

        Ok(())
    }
}
