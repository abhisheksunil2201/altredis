pub struct Replica {
    _id: u64,
    pub address: String,
    pub port: u16,
}

impl Replica {
    pub fn new(id: u64, address: String, port: u16) -> Self {
        Self {
            _id: id,
            address,
            port,
        }
    }
}
