use serde_json::{Value, Map};
/// Retrieves a nested field from a JSON object given a dot-separated path.
///
/// # Arguments
///
/// * `map` - A reference to a JSON object (`Map<String, Value>`).
/// * `field` - A dot-separated string specifying the path to the nested field.
///
/// # Returns
///
/// An `Option` containing a reference to the `Value` at the specified path if it exists, or `None` if the path does not exist.
///
/// # Examples
///
/// ```
/// use serde_json::{Value, Map};
/// // Assume `get_nested_field` is defined
///
/// let mut data = Map::new();
/// data.insert("foo".to_string(), Value::Object({
///     let mut inner = Map::new();
///     inner.insert("bar".to_string(), Value::String("baz".to_string()));
///     inner
/// }));
///
/// let result = get_nested_field(&data, "foo.bar");
/// assert_eq!(result, Some(&Value::String("baz".to_string())));
/// ```
///
/// # Notes
///
/// This function traverses the JSON object recursively based on the provided path. If at any point the path does not exist or the current value is not a JSON object when more path segments remain, `None` is returned.
pub(crate) fn get_nested_field<'a>(
    map: &'a Map<String, Value>,
    field: &str,
) -> Option<&'a Value> {
    let parts: Vec<&str> = field.split('.').collect();
    let mut current = map;
    for (i, part) in parts.iter().enumerate() {
        if let Some(value) = current.get(*part) {
            if i == parts.len() - 1 {
                return Some(value);
            } else if let Value::Object(ref obj) = value {
                current = obj;
            } else {
                return None;
            }
        } else {
            return None;
        }
    }
    None
}
