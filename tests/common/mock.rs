use std::{rc::Rc, slice};

use jbd_rs::sal::{BlockDevice, Buffer, System};
use rand::Rng;

use super::sal::UserSystem;

pub fn write_random_block(system: &UserSystem, dev: &Rc<dyn BlockDevice>, block_id: usize) -> Rc<dyn Buffer> {
    let buf = system.get_buffer_provider().get_buffer(dev, block_id).unwrap();
    let data = convert_buf(&buf);
    for b in data.iter_mut() {
        *b = rand::thread_rng().gen_range(0..256) as u8;
    }
    buf.mark_dirty();
    buf
}

pub fn validate_block(system: &UserSystem, dev: &Rc<dyn BlockDevice>, block_id: usize, buf: &Rc<dyn Buffer>) {
    let buf2 = system.get_buffer_provider().get_buffer(dev, block_id).unwrap();
    assert_eq!(convert_buf(&buf), convert_buf(&buf2));
}

fn convert_buf(buf: &Rc<dyn Buffer>) -> &mut [u8] {
    let data = unsafe { slice::from_raw_parts_mut(buf.data(), buf.size()) };
    data
}
