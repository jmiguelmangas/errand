from errand import RetryPolicy


def test_none_backoff_is_always_zero() -> None:
    policy = RetryPolicy(backoff="none", base_delay=5.0)
    assert policy.delay_for(1) == 0.0
    assert policy.delay_for(10) == 0.0


def test_fixed_backoff_is_constant() -> None:
    policy = RetryPolicy(backoff="fixed", base_delay=2.0)
    assert policy.delay_for(1) == 2.0
    assert policy.delay_for(2) == 2.0
    assert policy.delay_for(5) == 2.0


def test_fixed_backoff_capped_by_max_delay() -> None:
    policy = RetryPolicy(backoff="fixed", base_delay=10.0, max_delay=3.0)
    assert policy.delay_for(1) == 3.0


def test_exponential_backoff_doubles_each_attempt() -> None:
    policy = RetryPolicy(backoff="exponential", base_delay=1.0, max_delay=1000.0)
    assert policy.delay_for(1) == 1.0
    assert policy.delay_for(2) == 2.0
    assert policy.delay_for(3) == 4.0
    assert policy.delay_for(4) == 8.0


def test_exponential_backoff_capped_by_max_delay() -> None:
    policy = RetryPolicy(backoff="exponential", base_delay=1.0, max_delay=5.0)
    assert policy.delay_for(1) == 1.0
    assert policy.delay_for(2) == 2.0
    assert policy.delay_for(3) == 4.0
    assert policy.delay_for(4) == 5.0
    assert policy.delay_for(10) == 5.0


def test_default_policy_has_no_retries() -> None:
    policy = RetryPolicy()
    assert policy.max_retries == 0
    assert policy.backoff == "none"
