#![no_main]

use libfuzzer_sys::fuzz_target;
use wasi_pg_client::protocol::MessageBuffer;

fuzz_target!(|data: &[u8]| {
    // Whole-buffer backend decoding with the default safety cap.
    let mut buf = MessageBuffer::new();
    let _ = buf.try_extend(data);

    while let Ok(Some(_msg)) = buf.next_message() {
        // consume all decoded messages
    }
});
