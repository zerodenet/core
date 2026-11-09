use super::*;
#[test]
fn url_filter_rows_are_prepaid_and_keep_existing_first_parameter_trim_empty_semantics() {
    let book = ControlBudget::new(
        zero_runtime::configuration::ControlBudgetLimits::from_runtime(&Default::default()),
    );
    let (credit, filter) =
        input("types= flow.closed, ,stats.updated &types=ignored", &book).unwrap();
    assert_eq!(filter.event_types, ["flow.closed", "", "stats.updated"]);
    assert_eq!(filter.event_types.capacity(), 3);
    let paid = 3 * std::mem::size_of::<String>() + "flow.closed".len() + "stats.updated".len();
    assert_eq!(book.usage().request_bytes, paid);
    let mut limits = book.limits();
    limits.messages.request_bytes_total = paid - 1;
    assert!(book.prepare(limits).is_err());
    drop(filter);
    drop(credit);
    assert_eq!(book.usage().request_bytes, 0);
    book.prepare(limits).unwrap().commit().unwrap();
    assert!(input("types=flow.closed,,stats.updated", &book).is_err());
    assert_eq!(book.usage().request_bytes, 0);
}
