//! Integration tests against a real Redis (Testcontainers, `redis:7.4-alpine`).
//!
//! A custom harness (libtest-mimic) so the whole run shares one container and removes it when
//! the run ends; the standard harness has no "after all tests" hook. Filtering, `--list`,
//! `--test-threads` and the output format work as usual. Each test gets its own Tokio runtime,
//! its own Redis connection and its own sale (a random SALE_ID).

mod api;
mod concurrency;
mod demo_page;
mod maintenance_worker;
mod order_store;
mod queue_store;
mod support;
mod ticket_board;
mod ticket_events;

use libtest_mimic::{Arguments, Failed, Trial};

/// `module::test_name` for each listed async test function.
macro_rules! trials {
    ($($module:ident :: $name:ident),* $(,)?) => {
        vec![$(
            Trial::test(concat!(stringify!($module), "::", stringify!($name)), || run($module::$name))
        ),*]
    };
}

fn main() {
    let args = Arguments::from_args();
    let mut trials = trials![
        queue_store::join_puts_users_in_line_in_arrival_order,
        queue_store::join_again_returns_the_same_ticket_so_nobody_holds_two_places,
        queue_store::admit_lets_in_at_most_max_active_in_arrival_order,
        queue_store::admit_never_lets_in_more_users_than_stock_left,
        queue_store::admit_skips_users_who_stopped_polling,
        queue_store::admission_window_expires_and_frees_the_slot_for_the_next_user,
        queue_store::rejoin_after_admission_expired_goes_to_the_back_of_the_line,
        queue_store::rejoin_after_being_skipped_for_not_polling_gets_a_new_ticket,
        queue_store::waiting_ticket_estimates_the_wait_once_admissions_are_flowing,
        queue_store::wait_estimate_matches_a_steady_admission_rate,
        queue_store::get_ticket_returns_none_for_an_unknown_ticket,
        order_store::checkout_before_admission_is_rejected,
        order_store::checkout_reserves_stock_and_starts_the_payment_clock,
        order_store::checkout_repeated_returns_the_same_order_and_reserves_once,
        order_store::checkout_repeated_with_a_different_quantity_is_rejected,
        order_store::checkout_more_than_is_left_is_rejected_and_keeps_the_admission,
        order_store::checkout_while_unpaid_orders_hold_the_rest_is_not_sold_out_yet,
        order_store::checkout_after_the_admission_window_is_rejected,
        order_store::checkout_unknown_ticket_is_not_found,
        order_store::pay_charges_once_even_when_repeated,
        order_store::pay_after_the_deadline_expires_the_order_and_returns_the_stock,
        order_store::pay_unknown_order_is_not_found,
        order_store::maintenance_releases_unpaid_orders_and_the_stock_goes_to_the_next_in_line,
        order_store::rejoin_after_the_order_expired_gets_a_new_ticket_but_after_paying_does_not,
        order_store::sold_out_turns_away_newcomers_and_tells_the_line,
        concurrency::parallel_checkouts_never_oversell_nor_order_twice_per_ticket,
        concurrency::parallel_payments_charge_each_order_once,
        concurrency::parallel_joins_from_the_same_user_yield_one_ticket,
        concurrency::parallel_admitters_never_exceed_max_active,
        concurrency::payments_racing_the_expiry_sweep_return_each_orders_stock_once,
        concurrency::admitter_with_a_clock_slightly_behind_still_completes_its_pass,
        api::health_names_the_instance,
        api::full_flow_join_wait_checkout_pay,
        api::checkout_before_admission_is_403,
        api::checkout_after_the_admission_window_is_410,
        api::checkout_conflicts_are_reported_with_their_code,
        api::checkout_when_sold_out_is_409,
        api::join_when_sold_out_is_409,
        api::pay_after_the_deadline_is_410,
        api::field_names_match_case_insensitively_like_the_dotnet_version,
        api::redis_down_is_503_not_a_crash_and_recovers,
        api::connecting_to_an_unreachable_redis_fails_fast,
        api::unknown_routes_are_404_and_still_name_the_instance,
        api::a_reply_the_code_does_not_understand_is_a_500_with_the_error_body,
        demo_page::demo_page_is_served_at_root_with_a_fresh_script_nonce_each_time,
        demo_page::admin_config_shows_the_sale_settings_but_not_where_redis_is,
        ticket_board::list_shows_each_users_current_ticket_with_its_order,
        ticket_board::list_shows_only_the_new_ticket_after_a_user_lines_up_again,
        ticket_board::list_is_not_a_heartbeat_so_watchers_cannot_keep_a_gone_user_in_line,
        ticket_board::admin_tickets_returns_ticket_and_order_objects,
        ticket_events::watch_returns_the_views_and_counts_as_presence,
        ticket_events::watch_handles_more_tickets_than_one_batch,
        ticket_events::events_start_with_the_ticket_and_push_the_admission_right_away,
        ticket_events::events_carry_streaming_headers,
        ticket_events::events_end_once_the_ticket_can_no_longer_change,
        ticket_events::events_refresh_the_position_every_poll_interval,
        ticket_events::closing_a_stream_unregisters_it,
        ticket_events::admissions_are_pushed_again_once_the_subscription_comes_back,
        ticket_events::a_view_the_code_does_not_understand_stops_the_push_loop,
        maintenance_worker::worker_admits_on_every_tick_without_being_asked,
        maintenance_worker::worker_keeps_ticking_through_a_redis_outage,
    ];
    // One trial per malformed request, like the .NET [Theory] rows.
    trials.extend(api::ERROR_CASES.iter().map(|case| {
        Trial::test(
            format!("api::errors_use_status_code_and_error_body::{}", case.name),
            move || run(|| api::error_case(case)),
        )
    }));

    let conclusion = libtest_mimic::run(&args, trials);
    support::remove_redis();
    conclusion.exit();
}

/// Makes sure the shared Redis is up, then runs one test on a fresh runtime. Dropping the runtime
/// at the end stops whatever background loops the test started.
fn run<F: Future<Output = ()>>(test: impl FnOnce() -> F) -> Result<(), Failed> {
    support::redis_address();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(test());
    Ok(())
}
