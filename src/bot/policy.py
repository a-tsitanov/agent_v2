"""Who may ask, how much, and how many at once.

Pure and synchronous (except the gate's counter), so the rules can be
asserted without a database or a Telegram server. The handlers in
`commands.py` apply them in a fixed order — admission, then quota, then
the gate — and every outcome, including each refusal, becomes a row in
`bot_request`.
"""

from __future__ import annotations

from dataclasses import dataclass

# User-facing text lives here rather than in the handlers, so the wording
# is one thing to change and the handlers stay about control flow. Russian,
# matching `pipeline.DENIED_MESSAGE`.
UNKNOWN_MESSAGE = (
    "Вы не зарегистрированы. Заявка создана — попросите администратора "
    "одобрить ваш ID: {user_id}"
)
PENDING_MESSAGE = (
    "Заявка на доступ ещё не одобрена. Ваш ID: {user_id} — передайте его "
    "администратору."
)
BLOCKED_MESSAGE = "Доступ заблокирован."
QUOTA_MESSAGE = (
    "Дневной лимит запросов исчерпан ({used} из {quota}). "
    "Лимит обнуляется в полночь UTC."
)
BUSY_MESSAGE = (
    "Сейчас выполняются другие запросы — подождите минуту и повторите. "
    "Поиск занимает около полутора минут."
)


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str = ""
    message: str = ""


ALLOW = Decision(allowed=True)


def admit(user: dict | None, *, user_id: int = 0) -> Decision:
    """Whether this user may run anything at all.

    Fail closed: anything that is not an explicit `active` is a refusal.
    A missing row, an unknown status, a typo in the database — all deny.
    """
    if user is None:
        return Decision(False, "unknown", UNKNOWN_MESSAGE.format(user_id=user_id))
    status = user.get("status")
    if status == "active":
        return ALLOW
    if status == "blocked":
        return Decision(False, "blocked", BLOCKED_MESSAGE)
    # `pending` and anything unrecognised land here together, on purpose.
    return Decision(False, "pending", PENDING_MESSAGE.format(user_id=user_id))


def check_quota(used: int, quota: int) -> Decision:
    """Daily budget. ``quota <= 0`` means unlimited."""
    if quota <= 0:
        return ALLOW
    if used >= quota:
        return Decision(False, "quota", QUOTA_MESSAGE.format(used=used, quota=quota))
    return ALLOW


def is_admin(user: dict | None) -> bool:
    return bool(user) and user.get("role") == "admin"


class ConcurrencyGate:
    """A non-blocking cap on simultaneous heavy requests.

    NOT an `asyncio.Semaphore`: the design refuses over the cap rather
    than queueing, and a semaphore's `acquire()` waits. Queueing is the
    next chunk's job.

    A plain counter is correct here because aiogram runs single-threaded
    on one event loop — there is no preemption between the check and the
    increment, so no lock is needed.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._in_flight = 0

    @property
    def in_flight(self) -> int:
        return self._in_flight

    def try_acquire(self) -> bool:
        """Take a slot if one is free. ``limit <= 0`` means unlimited."""
        if self._limit > 0 and self._in_flight >= self._limit:
            return False
        self._in_flight += 1
        return True

    def release(self) -> None:
        # Clamped at zero: an extra release must not hand out a phantom
        # slot later.
        self._in_flight = max(0, self._in_flight - 1)


__all__ = [
    "ALLOW",
    "BLOCKED_MESSAGE",
    "BUSY_MESSAGE",
    "PENDING_MESSAGE",
    "QUOTA_MESSAGE",
    "UNKNOWN_MESSAGE",
    "ConcurrencyGate",
    "Decision",
    "admit",
    "check_quota",
    "is_admin",
]
