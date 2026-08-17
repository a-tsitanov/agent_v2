"""Admission, quota and the concurrency cap."""

from __future__ import annotations

from src.bot.policy import (
    ConcurrencyGate,
    admit,
    check_quota,
    is_admin,
)

# ── admission ────────────────────────────────────────────────────────


def test_unknown_user_is_refused_and_told_their_id():
    """They cannot be approved without it."""
    d = admit(None, user_id=777)
    assert not d.allowed
    assert d.reason == "unknown"
    assert "777" in d.message


def test_pending_user_is_refused():
    d = admit({"status": "pending"}, user_id=777)
    assert not d.allowed
    assert d.reason == "pending"
    assert "777" in d.message


def test_blocked_user_is_refused():
    d = admit({"status": "blocked"})
    assert not d.allowed
    assert d.reason == "blocked"


def test_active_user_is_admitted():
    assert admit({"status": "active"}).allowed


def test_unrecognised_status_is_refused_not_admitted():
    """Fail closed. A typo in the database, a status added later and not
    handled here, a half-written row — none of them may grant access."""
    for status in ("", "ACTIVE", "approved", None, "whatever"):
        assert not admit({"status": status}).allowed


# ── quota ────────────────────────────────────────────────────────────


def test_quota_allows_below_the_limit():
    assert check_quota(19, 20).allowed


def test_quota_refuses_at_the_limit():
    d = check_quota(20, 20)
    assert not d.allowed
    assert d.reason == "quota"
    assert "20" in d.message


def test_quota_refuses_above_the_limit():
    assert not check_quota(50, 20).allowed


def test_non_positive_quota_means_unlimited():
    assert check_quota(10_000, 0).allowed
    assert check_quota(10_000, -1).allowed


# ── roles ────────────────────────────────────────────────────────────


def test_is_admin():
    assert is_admin({"role": "admin"})
    assert not is_admin({"role": "client"})
    assert not is_admin(None)
    assert not is_admin({})


# ── concurrency ──────────────────────────────────────────────────────


def test_gate_admits_up_to_the_limit_then_refuses():
    gate = ConcurrencyGate(2)
    assert gate.try_acquire()
    assert gate.try_acquire()
    assert not gate.try_acquire()
    assert gate.in_flight == 2


def test_gate_frees_a_slot_on_release():
    gate = ConcurrencyGate(2)
    gate.try_acquire()
    gate.try_acquire()
    assert not gate.try_acquire()
    gate.release()
    assert gate.try_acquire()


def test_gate_does_not_leak_a_slot_when_the_work_raises():
    """The failure that wedges the bot permanently: two errored searches
    with no release and nobody can ask anything again."""
    gate = ConcurrencyGate(1)
    for _ in range(5):
        assert gate.try_acquire()
        try:
            raise RuntimeError("search blew up")
        except RuntimeError:
            pass
        finally:
            gate.release()
    assert gate.in_flight == 0
    assert gate.try_acquire()


def test_extra_release_cannot_manufacture_a_slot():
    """Clamped at zero — otherwise a double release makes the counter
    negative and the cap stops capping."""
    gate = ConcurrencyGate(1)
    gate.release()
    gate.release()
    assert gate.in_flight == 0
    assert gate.try_acquire()
    assert not gate.try_acquire()


def test_non_positive_limit_means_unlimited():
    gate = ConcurrencyGate(0)
    for _ in range(50):
        assert gate.try_acquire()
