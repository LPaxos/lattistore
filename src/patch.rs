use std::collections::HashMap;

pub type Key = String;
pub type Value = String;
pub type Patch = HashMap<Key, Value>;
pub type Transaction = Box<dyn Fn(&Patch) -> Patch + Send>;

// Can't declare const String because reasons
pub fn def_val() -> Value {
    "".to_string()
}
