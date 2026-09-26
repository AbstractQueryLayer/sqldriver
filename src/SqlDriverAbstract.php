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
     * The innermost transaction begun here and not finished yet.
     */
    protected ?TransactionInterface $transaction = null;

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
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $transaction                = $context instanceof TransactionAwareInterface ? $context->getTransaction() : null;

        if ($transaction !== null) {
            $this->joinTransaction($transaction);
        }

        return $this->realExecuteQuery($sql);
    }

    abstract protected function realCreateStatement(string $sql): SqlStatementInterface;

    #[\Override]
    public function createStatement(string $sql, ?object $context = null): SqlStatementInterface
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        return $this->realCreateStatement($sql);
    }

    abstract protected function realExecuteStatement(SqlStatementInterface $statement): ResultInterface;

    #[\Override]
    public function executeStatement(SqlStatementInterface $statement, array $params = [], ?object $context = null): ResultInterface
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $transaction                = $context instanceof TransactionAwareInterface ? $context->getTransaction() : null;

        if ($transaction !== null) {
            $this->joinTransaction($transaction);
        }



        return $this->realExecuteStatement($statement);
    }

    /**
     * Begins the transaction on this storage. A transaction with a parent becomes a savepoint in the
     * parent's transaction, and the parent begins here first if it has not yet; a transaction without a
     * parent begins a transaction of the connection. It commits or rolls back the storage when it finishes.
     * A savepoint rollback undoes everything the connection did since the savepoint, the parent's
     * statements included: a parent's statements run while a child is open belong to the child.
     *
     * @throws ConnectFailed
     * @throws StorageException when the parent is open here but the connection of this coroutine has no
     *                          transaction, or when a transaction without a parent begins inside an open one
     * @throws \LogicException when the transaction has finished or is already open on this storage
     */
    #[\Override]
    public function beginTransaction(TransactionInterface $transaction): void
    {
        if ($this->isDisconnected()) {
            $this->connect();
        }

        $key                        = $this->transactionKey();
        $parent                     = $transaction->getParentTransaction();
        $savepoint                  = null;

        if ($parent !== null && false === $parent->isTransactionOpened($key)) {
            $this->beginTransaction($parent);
        }

        if ($parent !== null) {
            // A SAVEPOINT outside a transaction succeeds and creates nothing: the rows would commit at once.
            if (false === $this->realInTransaction()) {
                throw new QueryException('The parent transaction is not open on the connection of this coroutine', 'SAVEPOINT');
            }

            $savepoint              = 'aql_' . \spl_object_id($transaction);
        }

        //
        // See: https://dev.mysql.com/doc/refman/9.0/en/savepoint.html
        //
        if ($savepoint === null) {
            $this->realBeginTransaction($transaction);
        } else {
            $this->realExecuteQuery('SAVEPOINT ' . $savepoint);
        }

        // Registered after the begin, so that a begin the server refused leaves nothing to finish.
        try {
            $transaction->openTransaction(
                $key,
                fn(bool $commit) => $this->finalizeTransaction($commit, $transaction, $parent, $savepoint)
            );
        } catch (\LogicException $exception) {
            $this->finalizeTransaction(false, $transaction, $parent, $savepoint);
            throw $exception;
        }

        $this->transaction          = $transaction;
    }

    /**
     * Makes the next statement run inside the transaction: begins it here, or checks that the server has
     * not ended it. A deadlock or an implicit commit ends it on the server, and a statement run after that
     * would commit on its own.
     *
     * @throws StorageException when the connection of this coroutine is no longer in a transaction
     */
    protected function joinTransaction(TransactionInterface $transaction): void
    {
        if (false === $transaction->isTransactionOpened($this->transactionKey())) {
            $this->beginTransaction($transaction);
            return;
        }

        if (false === $this->realInTransaction()) {
            throw new QueryException('The transaction is no longer open on the connection of this coroutine', '');
        }
    }

    abstract protected function isDisconnected(): bool;

    #[\Override]
    public function getTransaction(): ?TransactionInterface
    {
        return $this->transaction;
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
        return match ($basicQuery->getQueryAction()) {
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
     * Ends on the server what beginTransaction() began: the connection's transaction, or the savepoint.
     */
    protected function finalizeTransaction(
        bool $commit,
        TransactionInterface $transaction,
        ?TransactionInterface $parent,
        ?string $savepoint
    ): void {
        if ($savepoint === null) {
            $this->transaction      = null;
        } elseif ($this->transaction === $transaction) {
            $this->transaction      = $parent;
        }

        match (true) {
            $savepoint === null && $commit => $this->realCommit(),
            $savepoint === null     => $this->realRollback(),
            $commit                 => $this->realExecuteQuery('RELEASE SAVEPOINT ' . $savepoint),
            default                 => $this->realExecuteQuery('ROLLBACK TO SAVEPOINT ' . $savepoint),
        };
    }

    /**
     * Begins a transaction of the connection, at the isolation level of the given one when it names a level.
     */
    abstract protected function realBeginTransaction(TransactionInterface $transaction): void;

    abstract protected function realCommit(): void;

    /**
     * Whether the connection of this coroutine is inside a transaction, as the server reports it.
     */
    abstract protected function realInTransaction(): bool;

    abstract protected function realRollback(): void;

    abstract protected function normalizeException(\Throwable $exception, string $sql): StorageException;
}
