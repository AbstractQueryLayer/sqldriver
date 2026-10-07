<?php

declare(strict_types=1);

namespace IfCastle\AQL\SqlDriver;

use IfCastle\AQL\Transaction\TransactionInterface;
use IfCastle\AQL\Storage\Exceptions\QueryException;

/**
 * The transactions open on one connection, outermost first: the transaction of the connection, then
 * its savepoints in the order they were set.
 */
final class OpenTransactions
{
    /**
     * @var list<TransactionInterface>
     */
    private array $transactions     = [];

    /**
     * Savepoints that ended with one set before them, still registered on their transactions: true when
     * that one was released or committed, false when it was rolled back.
     *
     * @var \WeakMap<TransactionInterface, bool>
     */
    private \WeakMap $endedInside;

    private bool $endedByServer     = false;

    private ?\Throwable $finishFailure = null;

    public function markFinishFailed(\Throwable $failure): void
    {
        $this->finishFailure ??= $failure;
    }

    public function checkUsable(): void
    {
        if ($this->finishFailure !== null) {
            throw new QueryException('A transaction could not finish; this connection cannot accept more SQL', '', $this->finishFailure);
        }
    }

    /**
     * @param (\Closure(): bool)|null $isOwnerGone whether the coroutine these transactions belong to has
     *                                            ended; null when the connection belongs to no coroutine
     */
    public function __construct(private readonly ?\Closure $isOwnerGone = null)
    {
        $this->endedInside          = new \WeakMap();
    }

    /**
     * Whether the coroutine the connection belonged to has ended; the pool then rolled it back.
     */
    public function isOwnerGone(): bool
    {
        return $this->isOwnerGone !== null && ($this->isOwnerGone)();
    }

    public function add(TransactionInterface $transaction): void
    {
        $this->transactions[]       = $transaction;
    }

    public function contains(TransactionInterface $transaction): bool
    {
        return \in_array($transaction, $this->transactions, true);
    }

    public function isEmpty(): bool
    {
        return $this->transactions === [];
    }

    public function innermost(): ?TransactionInterface
    {
        return $this->transactions === [] ? null : $this->transactions[\array_key_last($this->transactions)];
    }

    /**
     * Removes the transaction and every transaction opened after it: ending a savepoint ends the
     * savepoints set after it, and ending the connection's transaction ends them all.
     *
     * @param bool $released whether the transaction was released or committed rather than rolled back
     */
    public function removeFrom(TransactionInterface $transaction, bool $released): void
    {
        $position                   = \array_search($transaction, $this->transactions, true);

        if ($position === false) {
            return;
        }

        foreach (\array_splice($this->transactions, $position) as $removed) {
            if ($removed !== $transaction) {
                $this->endedInside[$removed] = $released;
            }
        }

        if ($this->transactions === []) {
            $this->endedByServer    = false;
        }
    }

    /**
     * How the one the savepoint ended with ended: true when released or committed, false when rolled
     * back, null when the savepoint did not end that way.
     */
    public function endedWith(TransactionInterface $transaction): ?bool
    {
        return $this->endedInside[$transaction] ?? null;
    }

    /**
     * Records that the server rolled back the transaction of the connection, with every savepoint in it.
     */
    public function markEndedByServer(): void
    {
        if ($this->transactions !== []) {
            $this->endedByServer    = true;
        }
    }

    public function isEndedByServer(): bool
    {
        return $this->endedByServer;
    }
}
