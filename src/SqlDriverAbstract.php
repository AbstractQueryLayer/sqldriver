<?php

declare(strict_types=1);

namespace IfCastle\AQL\SqlDriver;

use IfCastle\AQL\Dsl\BasicQueryInterface;
use IfCastle\AQL\Dsl\Sql\Query\QueryInterface;
use IfCastle\AQL\Entity\EntityInterface;
use IfCastle\AQL\Executor\Ddl\DdlExecutorFactoryInterface;
use IfCastle\AQL\Executor\Ddl\DdlExecutorForSql;
use IfCastle\AQL\Executor\Ddl\DdlExecutorInterface;
use IfCastle\AQL\Executor\QueryExecutorInterface;
use IfCastle\AQL\Executor\QueryExecutorResolverInterface;
use IfCastle\AQL\Executor\SqlQueryExecutor;
use IfCastle\AQL\Generator\Ddl\EntityToTableFactoryInterface;
use IfCastle\AQL\Result\ResultInterface;
use IfCastle\AQL\Storage\ConnectableTelemetryInterface;
use IfCastle\AQL\Storage\Exceptions\ConnectFailed;
use IfCastle\AQL\Storage\Exceptions\DuplicateKeysException;
use IfCastle\AQL\Storage\Exceptions\QueryException;
use IfCastle\AQL\Storage\Exceptions\RecoverableException;
use IfCastle\AQL\Storage\Exceptions\ServerHasGoneAwayException;
use IfCastle\AQL\Storage\Exceptions\StorageException;
use IfCastle\AQL\Storage\QueryableTelemetryInterface;
use IfCastle\AQL\Storage\SqlStatementFactoryInterface;
use IfCastle\AQL\Storage\SqlStatementInterface;
use IfCastle\AQL\Storage\SqlStorageInterface;
use IfCastle\AQL\Transaction\TransactionAbleInterface;
use IfCastle\AQL\Transaction\TransactionAwareInterface;
use IfCastle\AQL\Transaction\TransactionInterface;
use IfCastle\DI\AutoResolverInterface;
use IfCastle\DI\ContainerInterface;
use IfCastle\DI\DisposableInterface;
use IfCastle\DI\Exceptions\ConfigException;

abstract class SqlDriverAbstract implements
    SqlStorageInterface,
    TransactionAbleInterface,
    SqlStatementFactoryInterface,
    EntityToTableFactoryInterface,
    DdlExecutorFactoryInterface,
    QueryExecutorResolverInterface,
    AutoResolverInterface,
    DisposableInterface
{
    protected ?string $storageName  = null;

    protected string $dsn;

    protected ?string $username     = null;

    protected ?string $password     = null;

    /**
     * @var array<string, string>
     */
    protected array $options;

    protected string $initialQueries    = '';

    protected int $reconnectAttempts    = 0;

    protected int $maxAttempts          = 3;

    /**
     * The transactions open on the connection, for a driver whose coroutines share one connection.
     */
    private ?OpenTransactions $openTransactions = null;

    private ?ContainerInterface $container = null;

    protected ?StorageException $lastError = null;

    protected ConnectableTelemetryInterface|null $telemetry = null;

    protected QueryableTelemetryInterface|null $queryTelemetry = null;

    /**
     * @throws ConfigException
     */
    public function __construct(array $config)
    {
        if (!\array_key_exists(self::OPTIONS, $config)) {
            $config[self::OPTIONS] = [];
        }

        if (!\array_key_exists('dsn', $config)) {
            throw new ConfigException('dsn is not defined');
        }

        foreach (['dsn', 'username', 'password', self::OPTIONS] as $name) {
            $this->$name            = $config[$name] ?? null;
        }

        if (!empty($config[self::MAX_ATTEMPTS])) {
            $this->maxAttempts      = (int) $config[self::MAX_ATTEMPTS];
        }

        if ($this->maxAttempts === 0) {
            $this->maxAttempts      = 1;
        }

        if (!empty($config[self::INITIAL_QUERIES])) {
            $this->initialQueries   = (string) $config[self::INITIAL_QUERIES];
        }
    }

    #[\Override]
    public function resolveDependencies(ContainerInterface $container): void
    {
        $this->container            = $container;
        $this->telemetry             = $container->findDependency(ConnectableTelemetryInterface::class);
        $this->queryTelemetry        = $container->findDependency(QueryableTelemetryInterface::class);
    }

    /**
     * @throws ConnectFailed
     */
    #[\Override]
    public function connect(): void
    {
        $exception                  = null;
        $lastException              = null;

        do {
            try {
                $this->connectionAttempt();
                $this->reconnectAttempts = 0;
                break;
            } catch (ConnectFailed $lastException) {
                ++$this->reconnectAttempts;
            }
        } while ($this->reconnectAttempts < $this->maxAttempts);

        if ($exception !== null) {
            throw $exception;
        }

        if ($this->isDisconnected()) {
            throw new ConnectFailed('Connection failed after ' . $this->reconnectAttempts . ' attempts', 0, $lastException);
        }

        if ($this->initialQueries !== '') {
            $this->realExecuteQuery($this->initialQueries);
        }
    }

    abstract protected function connectionAttempt(): void;

    abstract protected function realExecuteQuery(string $sql): ResultInterface;

    /**
     * @throws ConnectFailed
     * @throws RecoverableException
     * @throws DuplicateKeysException
     * @throws QueryException
     * @throws ServerHasGoneAwayException
     * @throws StorageException
     */
    #[\Override]
    public function executeSql(string $sql, ?object $context = null): ResultInterface
    {
        $this->openTransactions()->checkUsable();
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $transaction                = $context instanceof TransactionAwareInterface ? $context->getTransaction() : null;

        if ($transaction !== null) {
            $this->joinTransaction($transaction);
        }

        try {
            return $this->realExecuteQuery($sql);
        } catch (StorageException $exception) {
            $this->noteFailure($exception);
            throw $exception;
        }
    }

    abstract protected function realCreateStatement(string $sql): SqlStatementInterface;

    #[\Override]
    public function createStatement(string $sql, ?object $context = null): SqlStatementInterface
    {
        $this->openTransactions()->checkUsable();
        if ($this->isDisconnected()) {
            $this->connect();
        }

        return $this->realCreateStatement($sql);
    }

    abstract protected function realExecuteStatement(SqlStatementInterface $statement): ResultInterface;

    #[\Override]
    public function executeStatement(SqlStatementInterface $statement, array $params = [], ?object $context = null): ResultInterface
    {
        $this->openTransactions()->checkUsable();
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $transaction                = $context instanceof TransactionAwareInterface ? $context->getTransaction() : null;

        if ($transaction !== null) {
            $this->joinTransaction($transaction);
        }

        try {
            return $this->realExecuteStatement($statement);
        } catch (StorageException $exception) {
            $this->noteFailure($exception);
            throw $exception;
        }
    }

    /**
     * Records a failure that made the server roll back the transaction of the connection. The driver of
     * the connection still reports a transaction after such an error, so this record is what tells.
     */
    private function noteFailure(StorageException $exception): void
    {
        if ($this->endsTransaction($exception)) {
            $this->openTransactions()->markEndedByServer();
        }
    }

    /**
     * Begins the transaction on this storage. A transaction with a parent becomes a savepoint in the
     * parent's transaction, and the parent begins here first if it has not yet; a transaction without a
     * parent begins a transaction of the connection. It commits or rolls back the storage when it finishes.
     * A savepoint rollback undoes everything the connection did since the savepoint, the parent's
     * statements included: a parent's statements run while a child is open belong to the child.
     *
     * @throws ConnectFailed
     * @throws StorageException when the parent is open on the connection of another coroutine or is no longer
     *                          open on the server, or when a transaction without a parent begins inside an
     *                          open one
     * @throws \LogicException when the transaction has finished or is already open on this storage
     */
    #[\Override]
    public function beginTransaction(TransactionInterface $transaction): void
    {
        $this->openTransactions()->checkUsable();
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $key                        = $this->transactionKey();

        if ($transaction->isTransactionOpened($key)) {
            throw new \LogicException('The transaction is already open on this storage');
        }

        $open                       = $this->openTransactions();
        $parent                     = $transaction->getParentTransaction();

        if ($parent !== null && false === $parent->isTransactionOpened($key)) {
            $this->beginTransaction($parent);
        }

        $this->checkBeginsOn($open, $parent);
        $savepoint                  = $parent === null ? null : 'aql_' . \spl_object_id($transaction);

        //
        // See: https://dev.mysql.com/doc/refman/9.0/en/savepoint.html
        //
        if ($savepoint === null) {
            $this->realBeginTransaction($transaction);
        } else {
            $this->realExecuteQuery('SAVEPOINT ' . $savepoint);
        }

        $open->add($transaction);

        // Registered after the begin, so that a begin the server refused leaves nothing to finish.
        try {
            $transaction->openTransaction(
                $key,
                fn(bool $commit) => $this->finalizeTransaction($commit, $open, $transaction, $savepoint),
                fn() => $this->checkFinishesOn($open)
            );
        } catch (\LogicException $exception) {
            $this->finalizeTransaction(false, $open, $transaction, $savepoint);
            throw $exception;
        }
    }

    /**
     * @throws StorageException when the transaction cannot begin on the connection of this coroutine
     */
    private function checkBeginsOn(OpenTransactions $open, ?TransactionInterface $parent): void
    {
        if ($parent === null) {
            if (false === $open->isEmpty()) {
                throw new QueryException('A transaction without a parent cannot begin inside an open one', 'BEGIN');
            }

            return;
        }

        if (false === $open->contains($parent)) {
            throw new QueryException('The parent transaction is open on the connection of another coroutine', 'SAVEPOINT');
        }

        // A SAVEPOINT outside a transaction succeeds and creates nothing: the rows would commit at once.
        if ($open->isEndedByServer() || false === $this->realInTransaction()) {
            throw new QueryException('The parent transaction is no longer open on the server', 'SAVEPOINT');
        }
    }

    /**
     * Makes the next statement run inside the transaction: begins it here, or checks that the server has
     * not ended it. A deadlock or an implicit commit ends it on the server, and a statement run after that
     * would commit on its own.
     *
     * @throws StorageException when the transaction is open on the connection of another coroutine, or the
     *                          connection of this coroutine is no longer in a transaction
     */
    protected function joinTransaction(TransactionInterface $transaction): void
    {
        if (false === $transaction->isTransactionOpened($this->transactionKey())) {
            $this->beginTransaction($transaction);
            return;
        }

        $open                       = $this->openTransactions();

        if ($open->endedWith($transaction) !== null) {
            throw new QueryException('The savepoint has ended with an enclosing savepoint set before it', '');
        }

        if (false === $open->contains($transaction)) {
            throw new QueryException('The transaction is open on the connection of another coroutine', '');
        }

        if ($open->isEndedByServer() || false === $this->realInTransaction()) {
            throw new QueryException('The transaction is no longer open on the connection of this coroutine', '');
        }
    }

    abstract protected function isDisconnected(): bool;

    /**
     * The innermost transaction open on the connection of this coroutine, or null.
     */
    #[\Override]
    public function getTransaction(): ?TransactionInterface
    {
        return $this->openTransactions()->innermost();
    }

    /**
     * @throws ConnectFailed
     */
    #[\Override]
    public function quote(mixed $value): string
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        if (\is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }

        if (\is_int($value) || \is_float($value) || \is_null($value)) {
            return (string) $value;
        }

        if ($value instanceof \Stringable) {
            $value                  = (string) $value;
        } elseif ($value instanceof \DateTimeInterface) {
            $value                  = $value->format('Y-m-d H:i:s');
        }

        return $this->realQuote($value);
    }

    abstract protected function realQuote(string $value): string;

    #[\Override]
    public function escape(string $value): string
    {
        // Only for MySQL and SQLite
        return '`' . $value . '`';
    }

    /**
     * @throws ConnectFailed
     */
    #[\Override]
    public function lastInsertId(): string|int|float|null
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $id                         = $this->realLastInsertId();

        if ($id === false) {
            return null;
        }

        // Try to cast string to int
        if (\is_string($id) && (int) $id == $id) {
            return (int) $id;
        }

        return $id;
    }

    abstract protected function realLastInsertId(): mixed;

    #[\Override]
    public function getLastError(): ?StorageException
    {
        return $this->lastError;
    }

    #[\Override]
    public function disconnect(): void
    {
        $this->telemetry?->registerDisconnect($this);
    }

    #[\Override]
    public function getStorageName(): ?string
    {
        return $this->storageName;
    }

    #[\Override]
    public function setStorageName(string $storageName): static
    {
        $this->storageName          = $storageName;

        return $this;
    }

    #[\Override]
    public function resolveQueryExecutor(BasicQueryInterface $basicQuery, ?EntityInterface $entity = null): ?QueryExecutorInterface
    {
        $executor = match ($basicQuery->getQueryAction()) {
            QueryInterface::ACTION_COPY,
            QueryInterface::ACTION_SELECT,
            QueryInterface::ACTION_COUNT,
            QueryInterface::ACTION_INSERT,
            QueryInterface::ACTION_UPDATE,
            QueryInterface::ACTION_DELETE,
            QueryInterface::ACTION_REPLACE
                                    => new SqlQueryExecutor(),
            default                 => null
        };

        if ($executor !== null) {
            $executor->resolveDependencies($this->container ?? throw new \LogicException('Resolve storage dependencies before AQL queries'));
        }

        return $executor;
    }

    #[\Override]
    public function newDdlExecutor(string $entityName): DdlExecutorInterface
    {
        return new DdlExecutorForSql($entityName);
    }

    #[\Override]
    public function dispose(): void
    {
        $this->disconnect();
    }

    /**
     * The key a transaction registers this storage under: its name, or its identity when it has none.
     */
    protected function transactionKey(): string
    {
        return $this->storageName ?? static::class . '#' . \spl_object_id($this);
    }

    /**
     * The transactions open on the connection this coroutine uses. Here the driver has one connection for
     * every coroutine; a driver that gives each coroutine a connection of its own keeps a list per coroutine.
     */
    protected function openTransactions(): OpenTransactions
    {
        return $this->openTransactions ??= new OpenTransactions();
    }

    /**
     * @throws StorageException when the transaction belongs to another coroutine that is still running
     */
    private function checkFinishesOn(OpenTransactions $open): void
    {
        if ($open !== $this->openTransactions() && false === $open->isOwnerGone()) {
            throw new QueryException('A transaction finishes in the coroutine that began it', '');
        }

        if (false === $open->isOwnerGone()) {
            $open->checkUsable();
        }
    }

    /**
     * Ends on the server what beginTransaction() began: the connection's transaction, or the savepoint.
     * What already ended, with an enclosing savepoint, on the server, or with its coroutine, is not sent
     * again: finishing it the way it ended succeeds, finishing it the other way fails.
     *
     * @throws StorageException when the server refuses the statement, or when what already ended is
     *                          finished the other way
     */
    protected function finalizeTransaction(
        bool $commit,
        OpenTransactions $open,
        TransactionInterface $transaction,
        ?string $savepoint
    ): void {
        if (false === $open->isOwnerGone()) {
            $open->checkUsable();
        }

        try {
            $this->finishOnConnection($commit, $open, $transaction, $savepoint);
        } catch (\Throwable $exception) {
            // Keep uncertain state local to this connection, without closing a shared pool.
            if ($open->contains($transaction)) {
                $open->markFinishFailed($exception);
            }

            throw $exception;
        }
    }

    private function finishOnConnection(
        bool $commit,
        OpenTransactions $open,
        TransactionInterface $transaction,
        ?string $savepoint
    ): void {
        $enclosingReleased          = $open->endedWith($transaction);

        if ($enclosingReleased !== null) {
            if ($commit === $enclosingReleased) {
                return;
            }

            throw new QueryException($commit
                ? 'The savepoint was rolled back with an enclosing savepoint set before it'
                : 'The savepoint was released with an enclosing savepoint set before it and cannot roll back alone',
                $commit ? 'RELEASE SAVEPOINT' : 'ROLLBACK TO SAVEPOINT');
        }

        $endedByServer              = $open->isEndedByServer();
        $ownerGone                  = $open !== $this->openTransactions();

        if ($ownerGone) {
            $open->removeFrom($transaction, false);
            if ($commit) {
                throw new QueryException('The coroutine that began the transaction has ended, and the pool rolled it back', 'COMMIT');
            }

            return;
        }

        if ($endedByServer) {
            // PDO still reports the transaction of the connection after the error; a ROLLBACK clears that.
            if ($savepoint === null || $commit) {
                if ($this->realInTransaction()) {
                    $this->realRollback();
                }
            }

            $open->removeFrom($transaction, false);

            if ($commit) {
                throw new QueryException('The server rolled the transaction back', 'COMMIT');
            }

            return;
        }

        if (false === $commit) {
            // A server that already ended the transaction has nothing left to roll back.
            match (true) {
                $savepoint !== null => $this->realExecuteQuery('ROLLBACK TO SAVEPOINT ' . $savepoint),
                $this->realInTransaction() => $this->realRollback(),
                default             => null,
            };

            $open->removeFrom($transaction, false);

            return;
        }

        if ($savepoint !== null) {
            $this->realExecuteQuery('RELEASE SAVEPOINT ' . $savepoint);
            $open->removeFrom($transaction, true);
            return;
        }

        try {
            $this->realCommit();
            $open->removeFrom($transaction, true);
        } catch (\Throwable $exception) {
            // The transaction is finished for its caller, so a COMMIT the server did not take would leave
            // it open, and a pooled connection pinned, until the coroutine ends.
            if ($this->rollBackQuietly()) {
                $open->removeFrom($transaction, false);
            }
            throw $exception;
        }
    }

    /**
     * Rolls back a transaction still open on the server, for a caller that is already reporting an error.
     */
    private function rollBackQuietly(): bool
    {
        try {
            if ($this->realInTransaction()) {
                $this->realRollback();
            }

            return true;
        } catch (\Throwable) {
            // The connection is broken as well; the error of the caller says more than this one.
            return false;
        }
    }

    /**
     * Begins a transaction of the connection, at the isolation level of the given one when it names a level.
     */
    abstract protected function realBeginTransaction(TransactionInterface $transaction): void;

    abstract protected function realCommit(): void;

    /**
     * Whether the connection of this coroutine is inside a transaction, as the server last reported it.
     */
    abstract protected function realInTransaction(): bool;

    /**
     * Whether the server rolls back the transaction of the connection when a statement fails this way.
     */
    protected function endsTransaction(StorageException $exception): bool
    {
        return false;
    }

    abstract protected function realRollback(): void;

    abstract protected function normalizeException(\Throwable $exception, string $sql): StorageException;
}
