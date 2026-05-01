#![no_main]
use libfuzzer_sys::fuzz_target;
use pg_protocol::MessageBuffer;

fuzz_target!(|data: &[u8]| {
    // Should never panic, regardless of input
    let mut buf = MessageBuffer::new();
    buf.extend(data);
    while let Ok(Some(_msg)) = buf.next_message() {
        // consume all messages
    }
});
