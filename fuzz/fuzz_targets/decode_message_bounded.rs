#![no_main]

use libfuzzer_sys::fuzz_target;
use wasi_pg_client::protocol::MessageBuffer;

fuzz_target!(|data: &[u8]| {
    // Stress buffer-limit handling with adversarial cap sizes and mixed
    // checked/unchecked growth paths.
    let max = match data.first() {
        Some(first) => usize::from(*first),
        None => 0,
    };

    let mut buf = MessageBuffer::with_max_buffered_bytes(max);
    let payload = if data.is_empty() { data } else { &data[1..] };

    let midpoint = payload.len() / 2;
    let _ = buf.try_extend(&payload[..midpoint]);
    buf.extend(&payload[midpoint..]);

    while let Ok(Some(_msg)) = buf.next_message() {
        // consume any complete messages that fit within the configured bound
    }
});
