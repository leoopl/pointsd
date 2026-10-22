Table ledger_entries(member_id, payment_id, kind, points, month) with UNIQUE (payment_id, kind): the database itself
allows at most one award and one reversal per payment.
Award(entry, cap) is one transaction:
BEGIN; -- READ COMMITTED (default): every statement below takes a fresh snapshot once the lock is granted
  SELECT pg_advisory_xact_lock(hashtext($member_id));                          -- serialises writes for this member only
  SELECT points FROM ledger_entries WHERE payment_id = $1 AND kind = 'award';    -- found: COMMIT, return it
  SELECT COALESCE(SUM(points), 0) AS used FROM ledger_entries WHERE member_id = $2 AND month = $3 AND kind = 'award';
  INSERT INTO ledger_entries VALUES ($2, $1, 'award', LEAST($points, GREATEST($cap - used, 0)), $3);
COMMIT;
Reverse(payment_id) is one transaction:
  SELECT member_id, points, month FROM ledger_entries WHERE payment_id = $1 AND kind = 'award'; -- none: ErrNotAwarded, caller acks + logs
  INSERT INTO ledger_entries VALUES (member_id, $1, 'reversal', -points, month) ON CONFLICT (payment_id, kind) DO NOTHING;

The advisory lock turns "sum this member's month, then insert" into one step, so concurrent awards can't oversubscribe the cap, while different members take different locks and never wait on each other. It relies on READ COMMITTED: under REPEATABLE READ the snapshot would predate the lock and the SUM could be stale. The UNIQUE constraint holds even if two replicas both pass the existence check. The reversal copies points from the award row, so it removes exactly what was earned, cap cut included. A connection error surfaces as `ErrUnavailable` and the message is nacked. A retry after a commit that did succeed finds the row and is a no-op.
